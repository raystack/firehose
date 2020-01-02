package com.gojek.esb.latestSink.http.request.header;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import com.gojek.esb.consumer.EsbMessage;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BasicHeaderTest {

  @Mock
  List<EsbMessage> esbMessages;

  @Test
  public void shouldGenerateBaseHeader() {
    String headerConfig = "content-type:json";
    BasicHeader header = new BasicHeader(headerConfig);

    assertEquals("json", header.build().get("content-type"));
  }

  @Test
  public void shouldHandleMultipleHeader() {
    String headerConfig = "Authorization:auth_token,Accept:text/plain";
    BasicHeader baseHeaderGenerator = new BasicHeader(headerConfig);

    Map<String, String> header = baseHeaderGenerator.build();
    assertEquals("auth_token", header.get("Authorization"));
    assertEquals("text/plain", header.get("Accept"));
  }

  @Test
  public void shouldNotThrowNullPointerExceptionWhenHeaderConfigEmpty() {
    String headerConfig = "";
    BasicHeader baseHeaderGenerator = new BasicHeader(headerConfig);

    baseHeaderGenerator.build();
  }
}
