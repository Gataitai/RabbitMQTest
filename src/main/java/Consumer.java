import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class Consumer {
    private static final String QUEUE_NAME = "example_queue";
    private static final String EXCHANGE_NAME = "direct_exchange";
    private static final String ROUTING_KEY = QUEUE_NAME;
    private final ObjectMapper objectMapper;
    private final Channel channel;

    public Consumer() throws IOException, TimeoutException {
        this.objectMapper = new ObjectMapper();
        Connection connection = getConnection();
        this.channel = connection.createChannel();
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        Consumer consumer = new Consumer();
        consumer.run();
    }

    private void run() throws IOException {
        declareRabbitMQ();

        Thread terminationListener = createTerminationListener();
        terminationListener.start();

        DeliverCallback deliverCallback = createDeliverCallback();
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
    }

    private void declareRabbitMQ() throws IOException {
        // Declare the exchange
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        // Declare the queue
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        // Bind the queue to the exchange with the routing key
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
    }

    private Thread createTerminationListener() {
        return new Thread(() -> {
            System.out.println("Press 'q' to terminate the consumer.");
            Scanner scanner = new Scanner(System.in);
            while (true) {
                if (scanner.nextLine().equalsIgnoreCase("q")) {
                    System.out.println("Terminating consumer...");
                    System.exit(0);
                }
            }
        });
    }

    private DeliverCallback createDeliverCallback() {
        return (consumerTag, delivery) -> {
            Message receivedMessage = objectMapper.readValue(delivery.getBody(), Message.class);
            System.out.println(" [x] Received '" + receivedMessage.getText() + "'");

            handleMessageType(delivery, receivedMessage);
        };
    }

    private void handleMessageType(Delivery delivery, Message receivedMessage) throws IOException {
        if (receivedMessage.getMessageType() == MessageType.MESSAGE) {
            sendResponse(delivery);
        }
        // Add more cases for other message types if needed
    }

    private void sendResponse(Delivery delivery) throws IOException {
        Message receivedMessage = objectMapper.readValue(delivery.getBody(), Message.class);

        // Create the response message
        Message responseMessage = new Message(MessageType.OK, "hello back");
        byte[] responseBytes = objectMapper.writeValueAsBytes(responseMessage);

        // Set the correlationId in the properties of the response message
        AMQP.BasicProperties replyProperties = new AMQP.BasicProperties.Builder()
                .correlationId(delivery.getProperties().getCorrelationId())
                .build();

        // Publish the response message with the correlationId
        channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProperties, responseBytes);

        System.out.println(" [x] Sent response '" + responseMessage.getText() + "'");
    }

    private Connection getConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        return factory.newConnection();
    }
}
