package com.gojek.esb.serializer;

import com.gojek.esb.consumer.Message;
import com.gojek.esb.exception.DeserializerException;

/**
 * Serializer serialize Message into string format.
 */
public interface MessageSerializer {

  String serialize(Message message) throws DeserializerException;
}
