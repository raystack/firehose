package com.gojek.esb.sink.prometheus;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.PrometheusSinkConfig;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.SinkFactory;
import com.gojek.esb.sink.http.request.uri.UriParser;
import com.gojek.esb.sink.prometheus.request.PromRequest;
import com.gojek.esb.sink.prometheus.request.PromRequestCreator;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.util.Map;

public class PromSinkFactory implements SinkFactory {

    @Override
    public AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        PrometheusSinkConfig promSinkConfig = ConfigFactory.create(PrometheusSinkConfig.class, configuration);
        String promSchemaProtoClass = promSinkConfig.getInputSchemaProtoClass();

        Instrumentation instrumentation = new Instrumentation(statsDReporter, PromSinkFactory.class);

        CloseableHttpClient closeableHttpClient = newHttpClient(promSinkConfig);
        instrumentation.logInfo("HTTP connection established");

        ProtoParser protoParser = new ProtoParser(stencilClient, promSchemaProtoClass);

        UriParser uriParser = new UriParser(protoParser, promSinkConfig.getKafkaRecordParserMode());

        PromRequest request = new PromRequestCreator(statsDReporter, promSinkConfig, protoParser, uriParser).createRequest();

        return new PromSink(new Instrumentation(statsDReporter, PromSink.class),
                request,
                closeableHttpClient,
                stencilClient,
                promSinkConfig.getSinkPromRetryStatusCodeRanges(),
                promSinkConfig.getSinkPromRequestLogStatusCodeRanges()
        );
    }

    private CloseableHttpClient newHttpClient(PrometheusSinkConfig prometheusSinkConfig) {
        Integer maxHttpConnections = prometheusSinkConfig.getSinkPromMaxConnections();
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(prometheusSinkConfig.getSinkPromRequestTimeoutMs())
                .setConnectionRequestTimeout(prometheusSinkConfig.getSinkPromRequestTimeoutMs())
                .setConnectTimeout(prometheusSinkConfig.getSinkPromRequestTimeoutMs()).build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(maxHttpConnections);
        connectionManager.setDefaultMaxPerRoute(maxHttpConnections);
        HttpClientBuilder builder = HttpClients.custom().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig);

        return builder.build();
    }
}
