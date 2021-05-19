package io.odpf.firehose.sink.file;

import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.consumer.Message;

public interface MetadataFactory {
    DynamicMessage create(Message message);
}
