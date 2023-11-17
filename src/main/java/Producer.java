import Message.MessageType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import Message.*;
import com.rabbitmq.client.ConnectionFactory;

class Producer {
    private static final String QUEUE_NAME = "example_queue";
    private static final String EXCHANGE_NAME = "";
    private static final String ROUTING_KEY = QUEUE_NAME;

    public static void main(String[] args) throws IOException, TimeoutException {
        try (Connection connection = getConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            Thread terminationListener = new Thread(() -> {
                System.out.println("Press 'q' to terminate the producer.");
                Scanner scanner = new Scanner(System.in);
                while (true) {
                    if (scanner.nextLine().equalsIgnoreCase("q")) {
                        System.out.println("Terminating producer...");
                        System.exit(0);
                    }
                }
            });
            terminationListener.start();

            do {
                Message message = new Message(MessageType.MESSAGE, "hello");

                ObjectMapper objectMapper = new ObjectMapper();
                byte[] messageBytes = objectMapper.writeValueAsBytes(message);

                AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                        .correlationId(UUID.randomUUID().toString())
                        .replyTo(QUEUE_NAME)
                        .build();

                channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, properties, messageBytes);

                System.out.println(" [x] Sent '" + message.getText() + "'");
            } while (true);
        }
    }

    private static Connection getConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        return factory.newConnection();
    }
}