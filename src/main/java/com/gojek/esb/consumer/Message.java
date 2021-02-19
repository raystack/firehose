package com.gojek.esb.consumer;


import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.kafka.common.header.Headers;

import java.util.Base64;

/**
 * A class to hold a single protobuf message in binary format.
 */
@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class Message {
    private byte[] logKey;
    private byte[] logMessage;
    private String topic;
    private int partition;
    private long offset;
    private Headers headers;
    private long timestamp;
    private long consumeTimestamp;

    public Message(byte[] logKey, byte[] logMessage, String topic, int partition, long offset) {
        this.logKey = logKey;
        this.logMessage = logMessage;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public String getSerializedKey() {
        return encodedSerializedStringFrom(logKey);
    }

    public String getSerializedMessage() {
        return encodedSerializedStringFrom(logMessage);
    }

    private static String encodedSerializedStringFrom(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        return new String(Base64.getEncoder().encode(bytes));
    }
}
