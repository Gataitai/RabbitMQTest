import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;
import Message.*;
public class Consumer {
    private static final String QUEUE_NAME = "example_queue";
    private final ObjectMapper objectMapper;

    public Consumer() {
        this.objectMapper = new ObjectMapper();
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        Consumer consumer = new Consumer();
        consumer.run();
    }

    private void run() throws IOException, TimeoutException {
        try (Connection connection = getConnection();
             Channel channel = connection.createChannel()) {

            declareQueue(channel);

            Thread terminationListener = createTerminationListener();
            terminationListener.start();

            DeliverCallback deliverCallback = createDeliverCallback(channel);
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
            });
        }
    }

    private void declareQueue(Channel channel) throws IOException {
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
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

    private DeliverCallback createDeliverCallback(Channel channel) {
        return (consumerTag, delivery) -> {
            Message receivedMessage = objectMapper.readValue(delivery.getBody(), Message.class);
            System.out.println(" [x] Received '" + receivedMessage.getText() + "'");

            handleMessageType(channel, delivery, receivedMessage);
        };
    }

    private void handleMessageType(Channel channel, Delivery delivery, Message receivedMessage) throws IOException {
        switch (receivedMessage.getMessageType()) {
            case MESSAGE:
                sendResponse(channel, delivery);
                break;
            // Add more cases for other message types if needed
        }
    }

    private void sendResponse(Channel channel, Delivery delivery) throws IOException {
        Message responseMessage = new Message(MessageType.OK, "hello back");
        byte[] responseBytes = objectMapper.writeValueAsBytes(responseMessage);

        channel.basicPublish("", delivery.getProperties().getReplyTo(), null, responseBytes);

        System.out.println(" [x] Sent response '" + responseMessage.getText() + "'");
    }

    private Connection getConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        return factory.newConnection();
    }
}
