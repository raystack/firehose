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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ESSinkFactory implements SinkFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ESSinkFactory.class.getName());

    @Override
    public Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        ESSinkConfig esSinkConfig = ConfigFactory.create(ESSinkConfig.class, configuration);
        ESRequestHandler esRequestHandler = new ESRequestHandlerFactory(esSinkConfig, esSinkConfig.getEsIdFieldName(), esSinkConfig.getESMessageType(), new EsbMessageToJson(
                new ProtoParser(stencilClient, esSinkConfig.getProtoSchema()), esSinkConfig.shouldPreserveProtoFieldNames()),
                esSinkConfig.getEsTypeName(),
                esSinkConfig.getEsIndexName())
                .getRequestHandler();

        HttpHost[] httpHosts = getHttpHosts(esSinkConfig.getEsConnectionUrls());
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(httpHosts));
        return new ESSink(new Instrumentation(statsDReporter, ESSink.class), SinkType.ELASTICSEARCH.name(), client, esRequestHandler,
                esSinkConfig.getEsRequestTimeoutInMs(), esSinkConfig.getEsWaitForActiveShardsCount());
    }

    HttpHost[] getHttpHosts(String esConnectionUrls) {
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
            LOGGER.error("ES_CONNECTION_URLS is empty or null");
            throw new IllegalArgumentException("ES_CONNECTION_URLS is empty or null");
        }
    }
}
