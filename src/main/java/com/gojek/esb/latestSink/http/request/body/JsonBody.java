package com.gojek.esb.latestSink.http.request.body;

import java.util.ArrayList;
import java.util.List;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.serializer.EsbMessageSerializer;

/**
 * JsonBody Serialize the message according to injected serialzier and return it
 * as List of serialized string.
 */
public class JsonBody {

  private EsbMessageSerializer jsonSerializer;

  public JsonBody(EsbMessageSerializer jsonSerializer) {
    this.jsonSerializer = jsonSerializer;
  }

  public List<String> serialize(List<EsbMessage> esbMessages) throws DeserializerException {
    List<String> serializedBody = new ArrayList<String>();
    for (EsbMessage esbMessage : esbMessages) {
      serializedBody.add(jsonSerializer.serialize(esbMessage));
    }
    return serializedBody;
  }

}
