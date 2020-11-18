package com.gojek.esb.sink.elasticsearch;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.ESSinkConfig;
import com.gojek.esb.config.enums.SinkType;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.serializer.EsbMessageToJson;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.SinkFactory;
import com.gojek.esb.sink.elasticsearch.request.ESRequestHandler;
import com.gojek.esb.sink.elasticsearch.request.ESRequestHandlerFactory;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ESSinkFactory implements SinkFactory {

    @Override
    public Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        ESSinkConfig esSinkConfig = ConfigFactory.create(ESSinkConfig.class, configuration);

        Instrumentation instrumentation = new Instrumentation(statsDReporter, ESSinkFactory.class);
        String esConfig = String.format("\n\tES connection urls: %s\n\tES index name: %s\n\tES id field: %s\n\tES message type: %s"
                        + "\n\tES type name: %s\n\tES request timeout in ms: %s\n\tES retry status code blacklist: %s"
                        + "\n\tES routing key name: %s\n\tES wait for active shards count: %s\n\tES update only mode: %s"
                        + "\n\tES should preserve proto filed names: %s",
                esSinkConfig.getEsConnectionUrls(), esSinkConfig.getEsIndexName(), esSinkConfig.getEsIdFieldName(), esSinkConfig.getESMessageType(),
                esSinkConfig.getEsTypeName(), esSinkConfig.getEsRequestTimeoutInMs(), esSinkConfig.getEsRetryStatusCodeBlacklist(),
                esSinkConfig.getEsRoutingKeyName(), esSinkConfig.getEsWaitForActiveShardsCount(), esSinkConfig.isUpdateOnlyMode(),
                esSinkConfig.shouldPreserveProtoFieldNames());
        instrumentation.logDebug(esConfig);
        ESRequestHandler esRequestHandler = new ESRequestHandlerFactory(esSinkConfig, new Instrumentation(statsDReporter, ESRequestHandlerFactory.class),
                esSinkConfig.getEsIdFieldName(), esSinkConfig.getESMessageType(),
                new EsbMessageToJson(new ProtoParser(stencilClient, esSinkConfig.getProtoSchema()), esSinkConfig.shouldPreserveProtoFieldNames(), false),
                esSinkConfig.getEsTypeName(),
                esSinkConfig.getEsIndexName(),
                esSinkConfig.getEsRoutingKeyName())
                .getRequestHandler();

        HttpHost[] httpHosts = getHttpHosts(esSinkConfig.getEsConnectionUrls(), instrumentation);
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(httpHosts));
        instrumentation.logInfo("ES connection established");
        return new ESSink(new Instrumentation(statsDReporter, ESSink.class), SinkType.ELASTICSEARCH.name().toLowerCase(), client, esRequestHandler,
                esSinkConfig.getEsRequestTimeoutInMs(), esSinkConfig.getEsWaitForActiveShardsCount(), getStatusCodesAsList(esSinkConfig.getEsRetryStatusCodeBlacklist()));
    }

    HttpHost[] getHttpHosts(String esConnectionUrls, Instrumentation instrumentation) {
        if (esConnectionUrls != null && !esConnectionUrls.isEmpty()) {
            String[] esNodes = esConnectionUrls.trim().split(",");
            HttpHost[] httpHosts = new HttpHost[esNodes.length];
            for (int i = 0; i < esNodes.length; i++) {
                String[] node = esNodes[i].trim().split(":");
                if (node.length <= 1) {
                    throw new IllegalArgumentException("ES_CONNECTION_URLS should contain host and port both");
                }
                httpHosts[i] = new HttpHost(node[0].trim(), Integer.parseInt(node[1].trim()));
            }
            return httpHosts;
        } else {
            instrumentation.logError("No connection URL found");
            throw new IllegalArgumentException("ES_CONNECTION_URLS is empty or null");
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
