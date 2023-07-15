package org.raystack.firehose.serializer;

import org.raystack.firehose.message.Message;
import org.raystack.firehose.exception.DeserializerException;

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
