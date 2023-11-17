import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Producer {
    private static final String QUEUE_NAME = "example_queue";
    private static final String EXCHANGE_NAME = "direct_exchange";
    private static final String ROUTING_KEY = QUEUE_NAME;

    private final ObjectMapper objectMapper;
    private final String callbackQueue;
    private final Channel channel;

    public Producer() throws IOException, TimeoutException {
        this.objectMapper = new ObjectMapper();
        this.channel = createChannel();
        this.callbackQueue = channel.queueDeclare().getQueue();
        declareRabbitMQ();  // Declare the exchange and bind the queue in the constructor
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        Producer producer = new Producer();
        producer.run();
    }

    private void run() throws IOException, TimeoutException {
        Thread terminationListener = createTerminationListener();
        terminationListener.start();

        // Send a message and wait for the response with a timeout of 20 seconds
        sendMessageWithTimeout();
    }

    private Channel createChannel() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        return connection.createChannel();
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
            System.out.println("Press 'q' to terminate the producer or 's' to send a message.");
            Scanner scanner = new Scanner(System.in);
            while (true) {
                String input = scanner.nextLine();
                if (input.equalsIgnoreCase("q")) {
                    System.out.println("Terminating producer...");
                    System.exit(0);
                } else if (input.equalsIgnoreCase("s")) {
                    try {
                        sendMessageWithTimeout();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }


    private void sendMessageWithTimeout() throws IOException {
        // Generate a unique correlation ID for this message
        String correlationId = UUID.randomUUID().toString();

//        // Set up a CompletableFuture for handling the response
//        CompletableFuture<Void> responseFuture = new CompletableFuture<>();

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
            if(correlationId.equals(delivery.getProperties().getCorrelationId())){
                try {
                    Message responseMessage = objectMapper.readValue(delivery.getBody(), Message.class);
                    System.out.println(" [x] Received response '" + responseMessage.getText() + "' for Correlation ID: " + correlationId);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else {
                System.out.printf("correlationId's dont match. correlationId: " + correlationId + " - delivery: " + delivery.getProperties().getCorrelationId());
            }
//            responseFuture.complete(null); // Complete the CompletableFuture on receiving a response
        };

        channel.basicConsume(callbackQueue, true, deliverCallback, consumerTag -> {
        });

//        // Wait for the response with a timeout of 20 seconds
//        try {
//            responseFuture.get(20, TimeUnit.SECONDS);
//        } catch (InterruptedException | ExecutionException | TimeoutException e) {
//            System.out.println(" [!] Timeout: No response received within 20 seconds.");
//        }
    }
}
