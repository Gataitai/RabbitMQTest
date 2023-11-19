package common;

public class Message {
    private MessageType messageType;
    private String text;

    public Message(MessageType messageType, String text) {
        this.messageType = messageType;
        this.text = text;
    }

    public Message() {

    }

    public MessageType getMessageType() {
        return messageType;
    }

    public String getText() {
        return text;
    }
}
