package com.gojek.esb.sink.elasticsearch;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.EsSinkConfig;
import com.gojek.esb.config.enums.SinkType;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.serializer.MessageToJson;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.SinkFactory;
import com.gojek.esb.sink.elasticsearch.request.EsRequestHandler;
import com.gojek.esb.sink.elasticsearch.request.EsRequestHandlerFactory;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EsSinkFactory implements SinkFactory {

    @Override
    public Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        EsSinkConfig esSinkConfig = ConfigFactory.create(EsSinkConfig.class, configuration);

        Instrumentation instrumentation = new Instrumentation(statsDReporter, EsSinkFactory.class);
        String esConfig = String.format("\n\tES connection urls: %s\n\tES index name: %s\n\tES id field: %s\n\tES message type: %s"
                        + "\n\tES type name: %s\n\tES request timeout in ms: %s\n\tES retry status code blacklist: %s"
                        + "\n\tES routing key name: %s\n\tES wait for active shards count: %s\n\tES update only mode: %s"
                        + "\n\tES should preserve proto filed names: %s",
                esSinkConfig.getSinkEsConnectionUrls(), esSinkConfig.getSinkEsIndexName(), esSinkConfig.getSinkEsIdField(), esSinkConfig.getSinkEsInputMessageType(),
                esSinkConfig.getSinkEsTypeName(), esSinkConfig.getSinkEsRequestTimeoutMs(), esSinkConfig.getSinkEsRetryStatusCodeBlacklist(),
                esSinkConfig.getSinkEsRoutingKeyName(), esSinkConfig.getSinkEsShardsActiveWaitCount(), esSinkConfig.isSinkEsModeUpdateOnlyEnable(),
                esSinkConfig.isSinkEsPreserveProtoFieldNamesEnable());
        instrumentation.logDebug(esConfig);
        EsRequestHandler esRequestHandler = new EsRequestHandlerFactory(esSinkConfig, new Instrumentation(statsDReporter, EsRequestHandlerFactory.class),
                esSinkConfig.getSinkEsIdField(), esSinkConfig.getSinkEsInputMessageType(),
                new MessageToJson(new ProtoParser(stencilClient, esSinkConfig.getProtoSchema()), esSinkConfig.isSinkEsPreserveProtoFieldNamesEnable(), false),
                esSinkConfig.getSinkEsTypeName(),
                esSinkConfig.getSinkEsIndexName(),
                esSinkConfig.getSinkEsRoutingKeyName())
                .getRequestHandler();

        HttpHost[] httpHosts = getHttpHosts(esSinkConfig.getSinkEsConnectionUrls(), instrumentation);
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(httpHosts));
        instrumentation.logInfo("ES connection established");
        return new EsSink(new Instrumentation(statsDReporter, EsSink.class), SinkType.ELASTICSEARCH.name().toLowerCase(), client, esRequestHandler,
                esSinkConfig.getSinkEsRequestTimeoutMs(), esSinkConfig.getSinkEsShardsActiveWaitCount(), getStatusCodesAsList(esSinkConfig.getSinkEsRetryStatusCodeBlacklist()));
    }

    HttpHost[] getHttpHosts(String esConnectionUrls, Instrumentation instrumentation) {
        if (esConnectionUrls != null && !esConnectionUrls.isEmpty()) {
            String[] esNodes = esConnectionUrls.trim().split(",");
            HttpHost[] httpHosts = new HttpHost[esNodes.length];
            for (int i = 0; i < esNodes.length; i++) {
                String[] node = esNodes[i].trim().split(":");
                if (node.length <= 1) {
                    throw new IllegalArgumentException("sink.es.connection.urls should contain host and port both");
                }
                httpHosts[i] = new HttpHost(node[0].trim(), Integer.parseInt(node[1].trim()));
            }
            return httpHosts;
        } else {
            instrumentation.logError("No connection URL found");
            throw new IllegalArgumentException("sink.es.connection.urls is empty or null");
        }
    }

    List<String> getStatusCodesAsList(String esRetryStatusCodeBlacklist) {
        return Arrays
                .stream(esRetryStatusCodeBlacklist.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }
}
