package io.odpf.firehose.sink.prometheus;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import io.odpf.firehose.config.PrometheusSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.SinkFactory;
import io.odpf.firehose.sink.prometheus.request.PromRequest;
import io.odpf.firehose.sink.prometheus.request.PromRequestCreator;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;

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

        PromRequest request = new PromRequestCreator(statsDReporter, promSinkConfig, protoParser).createRequest();

        return new PromSink(new Instrumentation(statsDReporter, PromSink.class),
                request,
                closeableHttpClient,
                stencilClient,
                promSinkConfig.getSinkPromRetryStatusCodeRanges(),
                promSinkConfig.getSinkPromRequestLogStatusCodeRanges()
        );
    }

    private CloseableHttpClient newHttpClient(PrometheusSinkConfig prometheusSinkConfig) {
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(prometheusSinkConfig.getSinkPromRequestTimeoutMs())
                .setConnectionRequestTimeout(prometheusSinkConfig.getSinkPromRequestTimeoutMs())
                .setConnectTimeout(prometheusSinkConfig.getSinkPromRequestTimeoutMs()).build();
        BasicHttpClientConnectionManager connectionManager = new BasicHttpClientConnectionManager();
        HttpClientBuilder builder = HttpClients.custom().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig);

        return builder.build();
    }
}
