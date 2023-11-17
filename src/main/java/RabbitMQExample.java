

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;


public class RabbitMQExample {

    class Message {
        private String messageType;
        private String text;

        // Constructors, getters, setters

        public Message() {
            // Default constructor for Jackson deserialization
        }

        public Message(String messageType, String text) {
            this.messageType = messageType;
            this.text = text;
        }

        public String getMessageType() {
            return messageType;
        }

        public void setMessageType(String messageType) {
            this.messageType = messageType;
        }

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return "Message{" +
                    "messageType='" + messageType + '\'' +
                    ", text='" + text + '\'' +
                    '}';
        }
    }

    private static final String EXCHANGE_NAME = "message_exchange";
    private static final String ROUTING_KEY = "message_routing_key";
    ObjectMapper objectMapper = new ObjectMapper();

    public void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        // Create a producer
        ExecutorService producerExecutor = Executors.newSingleThreadExecutor();
        producerExecutor.execute(() -> {
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {

                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

                // Generate a random callback queue
                String callbackQueue = "callback_queue_" + Math.random();
                channel.queueDeclare(callbackQueue, false, true, true, null);

                // Create a message
                Message message = new Message("SENDING", "hello");
                byte[] messageBodyBytes = objectMapper.writeValueAsBytes(message);

                // Set up correlation ID
                String correlationId = java.util.UUID.randomUUID().toString();

                // Set up properties
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .correlationId(correlationId)
                        .replyTo(callbackQueue)
                        .build();

                // Publish the message
                channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, props, messageBodyBytes);
                System.out.println(" [x] Sent '" + message + "'");

            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        });

        // Create a consumer
        ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();
        consumerExecutor.execute(() -> {
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {

                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

                // Declare and bind the queue
                String queueName = "message_queue";
                channel.queueDeclare(queueName, false, false, false, null);
                channel.queueBind(queueName, EXCHANGE_NAME, ROUTING_KEY);

                // Set up the consumer
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String messageBody = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    Message receivedMessage = objectMapper.readValue(messageBody, Message.class);

                    // Simulate processing the message asynchronously
                    processMessageAsync(receivedMessage, delivery.getProperties().getCorrelationId());

                    // Send a response
                    Message responseMessage = new Message("OK", "hello");
                    byte[] responseMessageBytes = objectMapper.writeValueAsBytes(responseMessage);
                    channel.basicPublish("", delivery.getProperties().getReplyTo(), null, responseMessageBytes);
                };

                // Start consuming messages
                channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});

            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        });
    }

    private static void processMessageAsync(Message message, String correlationId) {
        // Simulate asynchronous processing
        ExecutorService processingExecutor = Executors.newSingleThreadExecutor();
        processingExecutor.execute(() -> {
            System.out.println(" [x] Processing message '" + message + "' with correlation ID '" + correlationId + "'");
            // Your actual processing logic here
        });
    }
}
