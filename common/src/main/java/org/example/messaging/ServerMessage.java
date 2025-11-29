package org.example.messaging;

import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import java.util.Objects;

public final class ServerMessage<T extends MessageLite> {

    private final T payload;
    private final String messageIndex;
    private final String messageId;

    public ServerMessage(T payload) {
        this.payload = Objects.requireNonNull(payload, "payload");
        this.messageIndex = MessageIndexHelper.computeIndex(payload);
        int senderId = MessageIndexHelper.extractSenderId(payload);
        this.messageId = (senderId != -1)
                ? messageIndex + "|" + senderId
                : messageIndex;
    }

    public T payload() {
        return payload;
    }

    public String getMessageIndex() {
        return messageIndex;
    }

    public String getMessageId() {
        return messageId;
    }

    @Override
    public String toString() {
        // Plug in your formatter as requested
        return "\nServerMessage{" +
                "\nid=" + messageId +
                ",\nindex=" + messageIndex +
                ",\npayload=" + ServerMessageFormatter.format(payload) +
                "}";
    }
}
