package com.gojek.esb.serializer;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;

/**
 * Serializer serialize EsbMessage into string format.
 */
public interface EsbMessageSerializer {

  String serialize(EsbMessage esbMessage) throws DeserializerException;
}
