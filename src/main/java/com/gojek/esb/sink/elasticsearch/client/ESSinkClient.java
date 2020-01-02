package com.gojek.esb.sink.elasticsearch.client;

import com.gojek.esb.config.ESSinkConfig;
import com.gojek.esb.sink.elasticsearch.BulkProcessorListener;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.function.BiConsumer;

import static org.elasticsearch.action.bulk.BackoffPolicy.exponentialBackoff;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

public class ESSinkClient {

    private final StatsDReporter statsDReporter;
    private RestHighLevelClient restHighLevelClient;
    private BulkProcessor bulkProcessor;
    private Instrumentation instrumentation;

    public ESSinkClient(ESSinkConfig esSinkConfig, StatsDReporter statsDReporter) {
        this.statsDReporter = statsDReporter;
        this.instrumentation = new Instrumentation(statsDReporter, ESSinkClient.class);
        HttpHost[] httpHosts = getHttpHosts(esSinkConfig.getEsConnectionUrls());
        if (httpHosts == null) {
            IllegalArgumentException e = new IllegalArgumentException("ES_CONNECTION_URLS is empty or null");
            instrumentation.captureFatalError(e);
            throw e;
        }
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> listenerBiConsumer = getBulkAsyncConsumer();
        this.restHighLevelClient = new RestHighLevelClient(RestClient.builder(httpHosts));
        bulkProcessor = buildBulkProcessor(esSinkConfig.getEsBatchSize(), esSinkConfig.getEsBatchRetryCount(),
                esSinkConfig.getEsRetryBackoff(), listenerBiConsumer);
    }

    public void processRequest(DocWriteRequest request) {
        getBulkProcessor().add(request);
    }

    public void close() {
        getBulkProcessor().close();
    }

    public RestHighLevelClient getRestHighLevelClient() {
        return restHighLevelClient;
    }

    private BulkProcessor getBulkProcessor() {
        return bulkProcessor;
    }

    private HttpHost[] getHttpHosts(String esConnectionUrls) {
        if (esConnectionUrls != null) {
            String[] esNodes = esConnectionUrls.trim().split(",");
            HttpHost[] httpHosts = new HttpHost[esNodes.length];
            for (int i = 0; i < esNodes.length; i++) {
                String[] node = esNodes[i].trim().split(":");
                httpHosts[i] = new HttpHost(node[0].trim(), Integer.parseInt(node[1].trim()));
            }
            return httpHosts;
        }
        return null;
    }

    private BiConsumer<BulkRequest, ActionListener<BulkResponse>> getBulkAsyncConsumer() {
        return (request, bulkListener) ->
                getRestHighLevelClient().bulkAsync(request, bulkListener);
    }

    private BulkProcessor buildBulkProcessor(int bulkActionsCount, int numberOfRetries, Long esRetryBackoff,
                                             BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer) {
        BulkProcessor.Listener bulkListener = getBulkListener();
        final long seconds = 15L;
        return BulkProcessor
                .builder(consumer, bulkListener)
                .setBulkActions(bulkActionsCount)
                .setConcurrentRequests(0)
                .setFlushInterval(timeValueSeconds(seconds))
                .setBackoffPolicy(exponentialBackoff(timeValueSeconds(esRetryBackoff), numberOfRetries))
                .build();
    }

    private BulkProcessor.Listener getBulkListener() {
        return new BulkProcessorListener(this.statsDReporter);
    }
}
