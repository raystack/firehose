package com.gojek.esb.sink.http.request.uri;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.proto.ProtoToFieldMapper;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ParametrizedUriTest {

  @Mock
  private ProtoToFieldMapper protoToFieldMapper;

  private EsbMessage esbMessage;

  @Before
  public void setup() {
    String logKey = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigC";
    String logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
    esbMessage = new EsbMessage(Base64.getDecoder().decode(logKey.getBytes()),
        Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);
  }

  @Test
  public void shouldAddParamMapToUri() {
    Map<String, Object> mockProtoField = Collections.singletonMap("order_number", (Object) "RB_1234");
    when(protoToFieldMapper.getFields(esbMessage.getLogMessage())).thenReturn(mockProtoField);

    ParameterizedUri parameterizedUri = new ParameterizedUri("http://dummy.com", protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE);

    try {
      URI actualUri = parameterizedUri.build(esbMessage);
      assertEquals(new URI("http://dummy.com?order_number=RB_1234"), actualUri);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void shouldHandleMultipleParam() {
    Map<String, Object> mockProtoField = new HashMap<String, Object>();
    mockProtoField.put("order_number", (Object) "RB_1234");
    mockProtoField.put("service_type", (Object) "GO_RIDE");

    when(protoToFieldMapper.getFields(esbMessage.getLogMessage())).thenReturn(mockProtoField);

    ParameterizedUri parameterizedUri = new ParameterizedUri("http://dummy.com", protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE);

    try {
      URI actualUri = parameterizedUri.build(esbMessage);
      assertEquals(new URI("http://dummy.com?service_type=GO_RIDE&order_number=RB_1234"), actualUri);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void shouldUseLogKeyWhenSourceParamTypeIsKey() {
    ParameterizedUri parameterizedUri = new ParameterizedUri("http://dummy.com", protoToFieldMapper, HttpSinkParameterSourceType.KEY);
    try {
      parameterizedUri.build(esbMessage);
      verify(protoToFieldMapper, times(1)).getFields(esbMessage.getLogKey());
    } catch (URISyntaxException e) {
      new RuntimeException(e);
    }
  }

  @Test
  public void shouldUseLogMessageWhenSourceParamTypeIsMessage() {
    ParameterizedUri parameterizedUri = new ParameterizedUri("http://dummy.com", protoToFieldMapper, HttpSinkParameterSourceType.MESSAGE);
    try {
      parameterizedUri.build(esbMessage);
      verify(protoToFieldMapper, times(1)).getFields(esbMessage.getLogMessage());
    } catch (URISyntaxException e) {
      new RuntimeException(e);
    }
  }
}
