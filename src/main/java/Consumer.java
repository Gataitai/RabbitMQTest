import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;
import Message.*;

public class Consumer {
    private static final String QUEUE_NAME = "example_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        try (Connection connection = getConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            Thread terminationListener = new Thread(() -> {
                System.out.println("Press 'q' to terminate the consumer.");
                Scanner scanner = new Scanner(System.in);
                while (true) {
                    if (scanner.nextLine().equalsIgnoreCase("q")) {
                        System.out.println("Terminating consumer...");
                        System.exit(0);
                    }
                }
            });
            terminationListener.start();

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                ObjectMapper objectMapper = new ObjectMapper();
                Message receivedMessage = objectMapper.readValue(delivery.getBody(), Message.class);
                System.out.println(" [x] Received '" + receivedMessage.getText() + "'");

                switch (receivedMessage.getMessageType()) {
                    case MESSAGE:
                        Message responseMessage = new Message(MessageType.OK, "hello back");

                        byte[] responseBytes = objectMapper.writeValueAsBytes(responseMessage);

                        channel.basicPublish("", delivery.getProperties().getReplyTo(), null, responseBytes);

                        System.out.println(" [x] Sent response '" + responseMessage.getText() + "'");
                        break;
                    // Add more cases for other message types if needed
                }
            };

            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
        }
    }

    private static Connection getConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        return factory.newConnection();
    }
}