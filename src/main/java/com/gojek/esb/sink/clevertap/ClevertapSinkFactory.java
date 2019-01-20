package com.gojek.esb.sink.clevertap;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.config.ClevertapSinkConfig;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoMessage;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.SinkFactory;
import com.gojek.esb.util.Clock;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.util.Map;

public class ClevertapSinkFactory implements SinkFactory {
    @Override
    public Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        ClevertapSinkConfig clevertapSinkConfig = ConfigFactory.create(ClevertapSinkConfig.class, configuration);
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(clevertapSinkConfig.getRequestTimeoutInMs())
                .setConnectionRequestTimeout(clevertapSinkConfig.getRequestTimeoutInMs())
                .setConnectTimeout(clevertapSinkConfig.getRequestTimeoutInMs())
                .build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(clevertapSinkConfig.getMaxHttpConnections());
        connectionManager.setDefaultMaxPerRoute(clevertapSinkConfig.getMaxHttpConnections());
        CloseableHttpClient closeableHttpClient = HttpClients.custom().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig).build();
        return new ClevertapSink(clevertapSinkConfig,
                new Clevertap(clevertapSinkConfig, closeableHttpClient, new Clock(), statsDReporter),
                new ProtoMessage(clevertapSinkConfig.getProtoSchema()));
    }
}
