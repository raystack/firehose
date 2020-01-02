package com.gojek.esb.latestSink.http.request.header;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Base64;
import java.util.Collections;
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
public class ParameterizedHeaderTest {

  @Mock
  ProtoToFieldMapper protoToFieldMapper;
  @Mock
  BasicHeader basicHeader;

  private EsbMessage esbMessage;

  @Before
  public void setup() {
    String logKey = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigC";
    String logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
    esbMessage = new EsbMessage(Base64.getDecoder().decode(logKey.getBytes()),
        Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);
  }

  @Test
  public void shouldHaveBaseHeader() {
    when(basicHeader.build()).thenReturn(Collections.singletonMap("content-type", "json"));
    ParameterizedHeader parameterizedHeader = new ParameterizedHeader(protoToFieldMapper,
        HttpSinkParameterSourceType.MESSAGE, basicHeader);

    Map<String, String> header = parameterizedHeader.build(esbMessage);
    assertEquals("json", header.get("content-type"));
  }

  @Test
  public void shouldHaveExtraParameterizedHeader() {
    Map<String, Object> mockParamMap = Collections.singletonMap("order_number", "RB_1234");
    when(protoToFieldMapper.getFields(esbMessage.getLogMessage())).thenReturn(mockParamMap);

    ParameterizedHeader parameterizedHeader = new ParameterizedHeader(protoToFieldMapper,
        HttpSinkParameterSourceType.MESSAGE, basicHeader);

    Map<String, String> header = parameterizedHeader.build(esbMessage);

    assertEquals("RB_1234", header.get("X-OrderNumber"));
  }

  @Test
  public void shouldUseLogKeyWhenSourceTypeIsKey() {
    ParameterizedHeader parameterizedHeader = new ParameterizedHeader(protoToFieldMapper,
        HttpSinkParameterSourceType.KEY, basicHeader);
    parameterizedHeader.build(esbMessage);
    verify(protoToFieldMapper, times(1)).getFields(esbMessage.getLogKey());
  }

  @Test
  public void shouldUseLogMessageWhenSourceTypeIsMessage() {
    ParameterizedHeader parameterizedHeader = new ParameterizedHeader(protoToFieldMapper,
        HttpSinkParameterSourceType.MESSAGE, basicHeader);
    parameterizedHeader.build(esbMessage);
    verify(protoToFieldMapper, times(1)).getFields(esbMessage.getLogMessage());
  }
}
