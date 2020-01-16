package com.gojek.esb.sink.http.request;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import com.gojek.de.stencil.client.StencilClient;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class RequestFactoryTest {
  @Mock
  private StencilClient stencilClient;

  private Map<String, String> configuration;

  @Before
  public void setup() {
    configuration = new HashMap<String, String>();
  }

  @Test
  public void shouldReturnBasicRequestWhenPrameterSourceIsDisabled() {
    Request request = new RequestFactory(configuration, stencilClient).create();

    assertTrue(request instanceof BatchRequest);
  }

  @Test
  public void shouldReturnParameterizedRequstWhenParameterSourceIsNotDisable() {
    configuration.put("HTTP_SINK_PARAMETER_SOURCE", "key");

    Request request = new RequestFactory(configuration, stencilClient).create();

    assertTrue(request instanceof ParameterizedRequest);
  }
}
