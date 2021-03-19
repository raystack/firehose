package io.odpf.firehose.serializer;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;

/**
 * Serializer serialize Message into string format.
 */
public interface MessageSerializer {

  String serialize(Message message) throws DeserializerException;
}
