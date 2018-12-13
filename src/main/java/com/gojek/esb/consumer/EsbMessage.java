package com.gojek.esb.consumer;


import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Base64;

/**
 * A class to hold a single protobuf message in binary format.
 */
@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class EsbMessage {
    private byte[] logKey;
    private byte[] logMessage;
    private String topic;
    private int partition;
    private long offset;

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
