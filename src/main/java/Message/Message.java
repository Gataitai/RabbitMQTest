package Message;

public class Message {
    private final MessageType messageType;
    private final String text;

    public Message(MessageType messageType, String text) {
        this.messageType = messageType;
        this.text = text;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public String getText() {
        return text;
    }
}
