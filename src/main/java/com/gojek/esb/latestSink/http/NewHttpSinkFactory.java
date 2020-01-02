package com.gojek.esb.latestSink.http;

import java.util.Map;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.SinkFactory;
import com.gojek.esb.latestSink.http.request.Request;
import com.gojek.esb.latestSink.http.request.RequestFactory;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;

import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class NewHttpSinkFactory implements SinkFactory {

  @Override
  public AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
    HTTPSinkConfig httpSinkConfig = ConfigFactory.create(HTTPSinkConfig.class, configuration);

    CloseableHttpClient closeableHttpClient = newHttpClient(httpSinkConfig);

    Instrumentation instrumentation = new Instrumentation(statsDReporter, HttpSink.class);

    Request request = new RequestFactory(configuration, stencilClient).create();

    return new HttpSink(instrumentation, request, closeableHttpClient, stencilClient);
  }

  private CloseableHttpClient newHttpClient(HTTPSinkConfig httpSinkConfig) {
    Integer maxHttpConnections = httpSinkConfig.getMaxHttpConnections();
    RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(httpSinkConfig.getRequestTimeoutInMs())
        .setConnectionRequestTimeout(httpSinkConfig.getRequestTimeoutInMs())
        .setConnectTimeout(httpSinkConfig.getRequestTimeoutInMs()).build();
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(maxHttpConnections);
    connectionManager.setDefaultMaxPerRoute(maxHttpConnections);
    return HttpClients.custom().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig).build();
  }
}
