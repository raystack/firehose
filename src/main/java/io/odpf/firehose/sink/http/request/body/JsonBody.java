package io.odpf.firehose.sink.http.request.body;

import java.util.ArrayList;
import java.util.List;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.serializer.MessageSerializer;

/**
 * JsonBody Serialize the message according to injected serialzier and return it
 * as List of serialized string.
 */
public class JsonBody {

  private MessageSerializer jsonSerializer;

  public JsonBody(MessageSerializer jsonSerializer) {
    this.jsonSerializer = jsonSerializer;
  }

  public List<String> serialize(List<Message> messages) throws DeserializerException {
    List<String> serializedBody = new ArrayList<String>();
    for (Message message : messages) {
      serializedBody.add(jsonSerializer.serialize(message));
    }
    return serializedBody;
  }

}
