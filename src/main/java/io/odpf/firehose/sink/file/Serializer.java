package io.odpf.firehose.sink.file;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;

public interface Serializer {
    Record serialize(Message message) throws DeserializerException;
}
