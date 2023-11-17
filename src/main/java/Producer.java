import Message.Message;
import Message.MessageType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class Producer {
    private static final String QUEUE_NAME = "example_queue";
    private static final String EXCHANGE_NAME = "";
    private static final String ROUTING_KEY = QUEUE_NAME;

    private final ObjectMapper objectMapper;
    private final String callbackQueue;

    public Producer() {
        this.objectMapper = new ObjectMapper();
        this.callbackQueue = createCallbackQueue();
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        Producer producer = new Producer();
        producer.run();
    }

    private void run() throws IOException, TimeoutException {
        try (Connection connection = getConnection();
             Channel channel = connection.createChannel()) {

            declareQueue(channel);
            declareCallbackQueue(channel);

            Thread terminationListener = createTerminationListener();
            terminationListener.start();

            // Send a message and wait for the response
            sendMessage(channel);
        }
    }

    private void declareQueue(Channel channel) throws IOException {
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    }

    private void declareCallbackQueue(Channel channel) throws IOException {
        channel.queueDeclare(callbackQueue, false, false, true, null);
    }

    private Thread createTerminationListener() {
        return new Thread(() -> {
            System.out.println("Press 'q' to terminate the producer.");
            Scanner scanner = new Scanner(System.in);
            while (true) {
                if (scanner.nextLine().equalsIgnoreCase("q")) {
                    System.out.println("Terminating producer...");
                    System.exit(0);
                }
            }
        });
    }

    private void sendMessage(Channel channel) throws IOException {
        // Generate a unique correlation ID for this message
        String correlationId = UUID.randomUUID().toString();

        // Send the message
        Message message = new Message(MessageType.MESSAGE, "hello");
        byte[] messageBytes = objectMapper.writeValueAsBytes(message);

        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .correlationId(correlationId)
                .replyTo(callbackQueue)
                .build();

        // Publish the message
        channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, properties, messageBytes);

        System.out.println(" [x] Sent '" + message.getText() + "' with Correlation ID: " + correlationId);

        // Set up the consumer for the specific correlation ID after publishing
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            if (correlationId.equals(delivery.getProperties().getCorrelationId())) {
                Message responseMessage = objectMapper.readValue(delivery.getBody(), Message.class);
                System.out.println(" [x] Received response '" + responseMessage.getText() + "' for Correlation ID: " + correlationId);
            }
        };

        channel.basicConsume(callbackQueue, true, deliverCallback, consumerTag -> {
        });
    }


    private Connection getConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        return factory.newConnection();
    }

    private String createCallbackQueue() {
        return "callback_queue_" + UUID.randomUUID().toString();
    }
}
