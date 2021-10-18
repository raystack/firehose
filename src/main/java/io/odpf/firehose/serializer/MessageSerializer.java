package io.odpf.firehose.serializer;

import io.odpf.firehose.type.Message;
import io.odpf.firehose.exception.DeserializerException;

/**
 * Serializer serialize Message into string format.
 */
public interface MessageSerializer {

  /**
   * Serialize kafka message into string.
   *
   * @param message the message
   * @return serialised message
   * @throws DeserializerException the deserializer exception
   */
  String serialize(Message message) throws DeserializerException;
}
