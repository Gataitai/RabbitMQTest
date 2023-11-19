import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import common.Message;
import common.MessageType;
import util.TaskSaver;

import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class Agent {
    private static final String QUEUE_NAME = "example_queue";
    private static final String EXCHANGE_NAME = "direct_exchange";
    private static final String ROUTING_KEY = QUEUE_NAME;
    private final ObjectMapper objectMapper;
    private final Channel channel;
    private final String callbackQueue;
    private final TaskSaver taskSaver;

    public Agent() throws IOException, TimeoutException {
        this.channel = createChannel();
        this.objectMapper = new ObjectMapper();
        this.callbackQueue = channel.queueDeclare().getQueue();
        this.taskSaver = new TaskSaver();
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        Agent agent = new Agent();
        agent.run();
    }

    private void run() throws IOException {
        declareRabbitMQ();

        Thread terminationListener = createTerminationListener();
        terminationListener.start();

        //consume on the main queue
        channel.basicConsume(QUEUE_NAME, true, (consumerTag, delivery) -> {
            Message receivedMessage = objectMapper.readValue(delivery.getBody(), Message.class);

            switch (receivedMessage.getMessageType()){
                case MESSAGE -> sendResponse(delivery);
                case OK -> System.out.println("OK " + receivedMessage.getText());
                case ERROR -> System.out.println("ERROR " + receivedMessage.getText());
            }

        }, consumerTag -> {});

        //consume on the callback queue
        channel.basicConsume(callbackQueue, true, (consumerTag, delivery) -> {
            String correlationId = delivery.getProperties().getCorrelationId();
            taskSaver.executeTask(correlationId, delivery);
        }, consumerTag -> {});
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
                        sendMessage();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    private void sendMessage() throws IOException {
        // Generate a unique correlation ID for this message
        String correlationId = UUID.randomUUID().toString();

        // Send the message
        Message message = new Message(MessageType.MESSAGE, "hello");
        byte[] messageBytes = objectMapper.writeValueAsBytes(message);

        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .correlationId(correlationId)
                .replyTo(callbackQueue)
                .build();

        //save a task together with the correlationId in advance.
        taskSaver.saveTask(correlationId, (delivery) -> {
            Message receivedMessage;
            try {
                receivedMessage = objectMapper.readValue(delivery.getBody(), Message.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println(receivedMessage.getText());
        });

        // Publish the message
        channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, properties, messageBytes);
        System.out.println(" [x] Sent '" + message.getText() + "' with Correlation ID: " + correlationId);
    }

    private void sendResponse(Delivery delivery) throws IOException {

        // Create the response message
        Message responseMessage = new Message(MessageType.OK, "hello back");
        byte[] responseBytes = objectMapper.writeValueAsBytes(responseMessage);

        // Set the correlationId in the properties of the response message
        AMQP.BasicProperties replyProperties = new AMQP.BasicProperties.Builder()
                .correlationId(delivery.getProperties().getCorrelationId())
                .build();

        // Publish the response message with the correlationId
        channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProperties, responseBytes);

        System.out.println(" [x] Sent response '" + responseMessage.getText() + "'. to id: " +delivery.getProperties().getCorrelationId());
    }

    private Channel createChannel() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        return connection.createChannel();
    }
}
