package io.odpf.firehose.sinkdecorator.dlq.objectstorage;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DlqMessage {
    private byte[] key;

    private byte[] value;
    private String topic;
    private int partition;
    private long offset;

    private long timestamp;
    private String error;
}
