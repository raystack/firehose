package com.gojek.esb.sink.elasticsearch.client;

import com.gojek.esb.config.ESSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.elasticsearch.BulkProcessorListener;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;
import java.util.function.BiConsumer;

import static org.elasticsearch.action.bulk.BackoffPolicy.exponentialBackoff;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

public class ESSinkClient {

    private BulkProcessorListener bulkProcessorListener;
    private final StatsDReporter statsDReporter;
    private RestHighLevelClient restHighLevelClient;
    private BulkProcessor bulkProcessor;
    private BiConsumer<BulkRequest, ActionListener<BulkResponse>> listenerBiConsumer;

    public ESSinkClient(ESSinkConfig esSinkConfig, StatsDReporter statsDReporter) {
        this.statsDReporter = statsDReporter;
        HttpHost[] httpHosts = getHttpHosts(esSinkConfig.getEsConnectionUrls());
        if (httpHosts == null) {
            throw new IllegalArgumentException("ES_CONNECTION_URLS is empty or null");
        }
        listenerBiConsumer = getBulkAsyncConsumer();
        this.restHighLevelClient = new RestHighLevelClient(RestClient.builder(httpHosts));
        bulkProcessor = buildBulkProcessor(esSinkConfig.getEsBatchSize(), esSinkConfig.getEsBatchRetryCount(),
                esSinkConfig.getEsRetryBackoff(), listenerBiConsumer);
    }

    public void processRequest(DocWriteRequest request) {
        bulkProcessor.add(request);
    }

    public void close() {
        bulkProcessor.close();
    }

    public RestHighLevelClient getRestHighLevelClient() {
        return restHighLevelClient;
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
        bulkProcessorListener = getBulkProcessorListener();
        final long seconds = 15L;
        return BulkProcessor
                .builder(consumer, bulkProcessorListener)
                .setBulkActions(bulkActionsCount)
                .setConcurrentRequests(0)
                .setFlushInterval(timeValueSeconds(seconds))
                .setBackoffPolicy(exponentialBackoff(timeValueSeconds(esRetryBackoff), numberOfRetries))
                .build();
    }

    private BulkProcessorListener getBulkProcessorListener() {
        Instrumentation instrumentation = new Instrumentation(this.statsDReporter, BulkProcessorListener.class);
        return new BulkProcessorListener(instrumentation);
    }

    public void updateEsbMessages(List<EsbMessage> esbMessages) {
        bulkProcessorListener.updateEsbMessages(esbMessages);
    }
}
