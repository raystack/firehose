package com.gojek.esb.sink.http.request.body;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.serializer.EsbMessageSerializer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JsonBodyTest {

  @Mock
  private EsbMessageSerializer esbMessageSerializer;

  private EsbMessage esbMessage;
  private List<EsbMessage> esbMessages;

  @Before
  public void setUp() {
    esbMessage = new EsbMessage(new byte[] {10, 20 }, new byte[] {1, 2 }, "sample-topic", 0, 100);
    esbMessages = Collections.singletonList(esbMessage);
  }

  @Test
  public void shouldReturnSameSizeOfBodyAsEsbMessage() {
    JsonBody jsonBody = new JsonBody(esbMessageSerializer);

    List<String> bodyContent;
    try {
      bodyContent = jsonBody.serialize(esbMessages);
    } catch (DeserializerException e) {
      throw new RuntimeException(e.toString());
    }
    assertEquals(1, bodyContent.size());
  }

  @Test
  public void shouldReturnSerializedValueOfMessage() {
    List<String> contentString;
    String mockSerializeResult = "{\"MockSerializer\": []}";
    try {
      when(esbMessageSerializer.serialize(esbMessage)).thenReturn(mockSerializeResult);

      JsonBody jsonBody = new JsonBody(esbMessageSerializer);
      contentString = jsonBody.serialize(esbMessages);

    } catch (DeserializerException e) {
      throw new RuntimeException(e.toString());
    }

    assertEquals(mockSerializeResult, contentString.get(0));
  }
}
