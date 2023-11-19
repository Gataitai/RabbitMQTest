import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import common.Message;
import common.MessageType;
import util.TaskSaver;

import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class Producer {
    private static final String QUEUE_NAME = "example_queue";
    private static final String EXCHANGE_NAME = "direct_exchange";
    private static final String ROUTING_KEY = QUEUE_NAME;

    private final ObjectMapper objectMapper;
    private final String callbackQueue;
    private final Channel channel;
    private final TaskSaver taskSaver;

    public Producer() throws IOException, TimeoutException {
        this.objectMapper = new ObjectMapper();
        this.channel = createChannel();
        this.callbackQueue = channel.queueDeclare().getQueue();
        this.taskSaver = new TaskSaver();

        // Declare the exchange
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        // Declare the queue
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        // Bind the queue to the exchange with the routing key
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        Producer producer = new Producer();
        producer.run();
    }

    private void run() throws IOException {
        Thread keyListener = createKeyListener();
        keyListener.start();

        //consume on the callbackqueue;
        channel.basicConsume(callbackQueue, true, (consumerTag, delivery) -> {
            String correlationId = delivery.getProperties().getCorrelationId();
            taskSaver.executeTask(correlationId, delivery);
        }, consumerTag -> {});

    }

    private Channel createChannel() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        return connection.createChannel();
    }

    private Thread createKeyListener() {
        return new Thread(() -> {
            String help = """
                Commands:

                L: List buildings
                R: Request reservation
                X: Cancel reservation
                C: Confirm reservation
                ?: This menu
                Q: Quit
                """;
            System.out.println(help);
            Scanner s = new Scanner(System.in);
            String c = s.nextLine().toLowerCase();
            while (true) {
                switch (c) {
                    case "?" -> System.out.println(help);
                    case "l" -> sendMessage();
                    case "r" -> System.out.println("r");
                    case "x" -> System.out.println("x");
                    case "c" -> System.out.println("c");
                    case "q" -> {
                        System.out.println("Terminating producer...");
                        System.exit(0);
                    }
                }
                c = s.nextLine().toLowerCase();
            }
        });
    }


    private void sendMessage(){
        try{
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
        }catch (Exception e){
            System.out.println(e);
        }
    }
}
