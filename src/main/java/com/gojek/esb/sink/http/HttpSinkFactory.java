package com.gojek.esb.sink.http;

import com.gojek.de.stencil.StencilClient;
import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.config.ParameterizedHTTPSinkConfig;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.parser.ProtoParser;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.SinkFactory;
import com.gojek.esb.sink.http.client.BasicHttpSinkClient;
import com.gojek.esb.sink.http.client.Header;
import com.gojek.esb.sink.http.client.ParameterizedHttpSinkClient;
import com.gojek.esb.sink.http.client.deserializer.JsonWrapperDeserializer;
import com.gojek.esb.util.Clock;
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
    public Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {

        HTTPSinkConfig httpSinkConfig = ConfigFactory.create(HTTPSinkConfig.class, configuration);
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(httpSinkConfig.getRequestTimeoutInMs())
                .setConnectionRequestTimeout(httpSinkConfig.getRequestTimeoutInMs())
                .setConnectTimeout(httpSinkConfig.getRequestTimeoutInMs())
                .build();
        CloseableHttpClient closeableHttpClient = newHttpClient(httpSinkConfig.getMaxHttpConnections(), requestConfig);
        Clock clock = new Clock();

        if ((httpSinkConfig.getHttpSinkParameterSource() != DISABLED) && (httpSinkConfig.getHttpSinkParameterSource() != null)) {
            ParameterizedHTTPSinkConfig parameterizedHttpSinkConfig = ConfigFactory.create(ParameterizedHTTPSinkConfig.class, configuration);
            return newParameterizedHttpSink(parameterizedHttpSinkConfig, closeableHttpClient, clock, statsDReporter, stencilClient);
        } else {
            return newHttpSink(httpSinkConfig, closeableHttpClient, clock, statsDReporter);
        }

    }

    private CloseableHttpClient newHttpClient(Integer maxHttpConnections, RequestConfig requestConfig) {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(maxHttpConnections);
        connectionManager.setDefaultMaxPerRoute(maxHttpConnections);
        return HttpClients.custom().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig).build();
    }

    private ParameterizedHttpSink newParameterizedHttpSink(ParameterizedHTTPSinkConfig config, CloseableHttpClient closeableHttpClient,
                                                           Clock clock, StatsDReporter statsDReporter, StencilClient stencilClient) {
        ProtoParser protoParser = new ProtoParser(stencilClient, config.getProtoSchema());
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, config.getProtoToFieldMapping());

        ParameterizedHttpSinkClient httpClient = new ParameterizedHttpSinkClient(config.getServiceURL(),
                new Header(config.getHTTPHeaders()), new JsonWrapperDeserializer(), protoToFieldMapper,
                config.getHttpSinkParameterSource(), config.getHttpSinkParameterPlacement(),
                closeableHttpClient, clock, statsDReporter);
        return new ParameterizedHttpSink(httpClient, config.retryStatusCodeRanges());
    }

    private HttpSink newHttpSink(HTTPSinkConfig config, CloseableHttpClient closeableHttpClient, Clock clock, StatsDReporter statsDReporter) {
        BasicHttpSinkClient httpClient = new BasicHttpSinkClient(config.getServiceURL(),
                new Header(config.getHTTPHeaders()), new JsonWrapperDeserializer(), closeableHttpClient, clock, statsDReporter);
        return new HttpSink(httpClient, config.retryStatusCodeRanges());
    }

}
