package com.gojek.esb.latestSink.http;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.config.ParameterizedHTTPSinkConfig;
import com.gojek.esb.config.enums.HttpSinkDataFormat;
import com.gojek.esb.latestSink.AbstractSink;
import com.gojek.esb.latestSink.SinkFactory;
import com.gojek.esb.latestSink.http.client.Header;
import com.gojek.esb.latestSink.http.client.deserializer.Deserializer;
import com.gojek.esb.latestSink.http.client.deserializer.JsonDeserializer;
import com.gojek.esb.latestSink.http.client.deserializer.JsonWrapperDeserializer;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoToFieldMapper;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.util.Map;

import static com.gojek.esb.config.enums.HttpSinkParameterSourceType.DISABLED;

/**
 * Factory class to create the HTTP Sink.
 * The esb-log-consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient client)}
 * to obtain the HTTPSink sink implementation. {@see ParameterizedHTTPSinkConfig}
 */
public class HttpSinkFactory implements SinkFactory {

    @Override
    public AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {

        HTTPSinkConfig httpSinkConfig = ConfigFactory.create(HTTPSinkConfig.class, configuration);
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(httpSinkConfig.getRequestTimeoutInMs())
                .setConnectionRequestTimeout(httpSinkConfig.getRequestTimeoutInMs())
                .setConnectTimeout(httpSinkConfig.getRequestTimeoutInMs())
                .build();
        CloseableHttpClient closeableHttpClient = newHttpClient(httpSinkConfig.getMaxHttpConnections(), requestConfig);

        Deserializer deserializer = (httpSinkConfig.getHttpSinkDataFormat() == HttpSinkDataFormat.JSON)
                ? new JsonDeserializer(new ProtoParser(stencilClient, httpSinkConfig.getProtoSchema()))
                : new JsonWrapperDeserializer();

        if (httpSinkConfig.getHttpSinkParameterSource() != DISABLED) {
            ParameterizedHTTPSinkConfig parameterizedHttpSinkConfig = ConfigFactory.create(ParameterizedHTTPSinkConfig.class, configuration);
            return newParameterizedHttpSink(parameterizedHttpSinkConfig, closeableHttpClient, deserializer, statsDReporter, stencilClient);
        } else {
            return newHttpSink(httpSinkConfig, closeableHttpClient, deserializer, statsDReporter, stencilClient);
        }

    }

    private CloseableHttpClient newHttpClient(Integer maxHttpConnections, RequestConfig requestConfig) {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(maxHttpConnections);
        connectionManager.setDefaultMaxPerRoute(maxHttpConnections);
        return HttpClients.custom().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig).build();
    }

    private LatestParameterizedHttpSink newParameterizedHttpSink(ParameterizedHTTPSinkConfig config, CloseableHttpClient closeableHttpClient, Deserializer deserializer,
                                                                 StatsDReporter statsDReporter, StencilClient stencilClient) {
        ProtoParser protoParser = new ProtoParser(stencilClient, config.getParameterProtoSchema());
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, config.getProtoToFieldMapping());

        return new LatestParameterizedHttpSink(new Instrumentation(statsDReporter, LatestParameterizedHttpSink.class), "http", protoToFieldMapper, config.getHttpSinkParameterSource(), deserializer, config.getServiceURL(), config.getHttpSinkParameterPlacement(), new Header(config.getHTTPHeaders()), closeableHttpClient, stencilClient);
    }

    private LatestHttpSink newHttpSink(HTTPSinkConfig config, CloseableHttpClient closeableHttpClient, Deserializer deserializer,
                                       StatsDReporter statsDReporter, StencilClient stencilClient) {
        return new LatestHttpSink(new Instrumentation(statsDReporter, LatestHttpSink.class), "http", deserializer, config.getServiceURL(), new Header(config.getHTTPHeaders()), closeableHttpClient, stencilClient);
    }



}
