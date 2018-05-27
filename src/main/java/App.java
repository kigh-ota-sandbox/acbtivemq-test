import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class App {
    public static void main(String[] args) {
        thread(new HelloWorldProducer(), false);
        thread(new HelloWorldProducer(), false);
        thread(new HelloWorldConsumer(), false);
    }

    private static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }

    private static class HelloWorldProducer implements Runnable {
        @Override
        public void run() {
            try {
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
                Connection connection = connectionFactory.createConnection();
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                Destination destination = session.createQueue("TEST.FOO");

                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                String text = "Hello From: " + Thread.currentThread().getName() + " : " + this.hashCode();
                TextMessage message = session.createTextMessage(text);

                producer.send(message);
                System.out.println("Sent message: " + message.hashCode() + " : "  + Thread.currentThread().getName());

                session.close();
                connection.close();
            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }
    }

    private static class HelloWorldConsumer implements Runnable, ExceptionListener {
        @Override
        public void run() {
            try {
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");

                Connection connection = connectionFactory.createConnection();
                connection.start();

                connection.setExceptionListener(this);

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                Destination destination = session.createQueue("TEST.FOO");

                MessageConsumer consumer = session.createConsumer(destination);

                Message message = consumer.receive(1000);

                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    String text = textMessage.getText();
                    System.out.println("Received: " + text);
                } else {
                    System.out.println("Received: " + message);
                }

                consumer.close();
                session.close();
                connection.close();

            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }

        @Override
        public synchronized void onException(JMSException exception) {
            System.out.println("JMS Exception occurred.. Shutting down client.");
        }
    }
}
