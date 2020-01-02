package com.gojek.esb.latestSink.http.serializer;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;

/**
 * Serializer
 */
public interface EsbMessageSerializer {

  public String serialize(EsbMessage esbMessage) throws DeserializerException;
}