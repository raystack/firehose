package com.gojek.esb.serializer;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * JsonWrappedProto wrap encoded64 protobuff message into json format. The
 * format would look like
 * <pre>
 * {
 *   "topic":"sample-topic",
 *   "log_key":"CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d",
 *   "log_message":"CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d"
 * }
 * </pre>
 */
public class JsonWrappedProtoByte implements EsbMessageSerializer {

  private Gson gson;

  public JsonWrappedProtoByte() {
    this.gson = new GsonBuilder().registerTypeAdapter(EsbMessage.class, new EsbMessageJsonSerializer()).create();
  }

  @Override
  public String serialize(EsbMessage esbMessage) throws DeserializerException {
    return gson.toJson(esbMessage);
  }

}
