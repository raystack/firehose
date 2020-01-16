package com.gojek.esb.sink.http.request;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.header.ParameterizedHeader;
import com.gojek.esb.sink.http.request.uri.ParameterizedUri;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MultipleRequestTest {
  @Mock
  private ParameterizedUri parameterizedUri;
  @Mock
  private ParameterizedHeader parameterizedHeader;
  @Mock
  private JsonBody jsonBody;

  @Test
  public void shouldCreateRequestsPerEsbMessage() {
    EsbMessage esbMessage = new EsbMessage(new byte[] {10, 20 }, new byte[] {1, 2 }, "sample-topic", 0, 100);
    List<EsbMessage> esbMessages = new ArrayList<EsbMessage>();
    esbMessages.add(esbMessage);
    esbMessages.add(esbMessage);

    try {
      List<String> bodyContent = new ArrayList<String>();
      bodyContent.add("{}");
      bodyContent.add("{}");
      when(jsonBody.serialize(esbMessages)).thenReturn(bodyContent);
    } catch (DeserializerException e) {
      throw new RuntimeException(e);
    }

    ParameterizedRequest multipleRequest = new ParameterizedRequest(parameterizedUri, parameterizedHeader, jsonBody);

    try {
      multipleRequest.build(esbMessages);
      verify(parameterizedUri, times(4)).build(esbMessage);
      verify(parameterizedHeader, times(4)).build(esbMessage);
      verify(jsonBody, times(1)).serialize(esbMessages);
    } catch (URISyntaxException | DeserializerException e) {
      throw new RuntimeException(e);
    }
  }
}
