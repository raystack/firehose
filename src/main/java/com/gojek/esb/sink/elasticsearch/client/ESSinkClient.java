package com.gojek.esb.sink.elasticsearch.client;

//CHECKSTYLE:OFF
import com.gojek.esb.config.ESSinkConfig;
import com.gojek.esb.metrics.StatsDReporter;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.function.BiConsumer;

import static com.gojek.esb.metrics.Metrics.*;

public class ESSinkClient {

    // Needs the ES High level restHighLevelClient
    // Needs the bulk processor which will execute the bulk requests

    private static final Logger LOGGER = LoggerFactory.getLogger(ESSinkClient.class.getName());
    private final StatsDReporter statsDReporter;

    private RestHighLevelClient restHighLevelClient;
    private BulkProcessor bulkProcessor;

    public ESSinkClient(ESSinkConfig esSinkConfig, StatsDReporter client) {
        this.statsDReporter = client;
        HttpHost[] httpHosts = getHttpHosts(esSinkConfig.getEsConnectionUrls());
        if (httpHosts == null) {
            LOGGER.error("ES_CONNECTION_URLS is empty or null");
            throw new IllegalArgumentException("ES_CONNECTION_URLS is empty or null");
        }
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> listenerBiConsumer = getBulkAsyncConsumer();
        this.restHighLevelClient = new RestHighLevelClient(RestClient.builder(httpHosts));
        bulkProcessor = buildBulkProcessor(esSinkConfig.getEsBatchSize(), listenerBiConsumer, esSinkConfig.getEsBatchRetryCount());
    }

    // A way to create the bulkprocessor once
    // A way to create the restHighLevelClient and submit the requests
    // a way to close the bulk processor when the firehose is closed

    public BulkProcessor getBulkProcessor() {
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

    private BulkProcessor buildBulkProcessor(int bulkActions, BiConsumer<BulkRequest,
            ActionListener<BulkResponse>> consumer, int numberOfRetries) {
        BulkProcessor.Listener bulkListener = getBulkListener();
        final long seconds = 15L;
        return BulkProcessor
                .builder(consumer, bulkListener)
                .setBulkActions(bulkActions)
                .setConcurrentRequests(0)
                .setFlushInterval(TimeValue.timeValueSeconds(seconds))
                .setBackoffPolicy(BackoffPolicy
                        .constantBackoff(TimeValue.timeValueSeconds(1L), numberOfRetries))
                .build();
    }

    public RestHighLevelClient getRestHighLevelClient() {
        return restHighLevelClient;
    }

    private BulkProcessor.Listener getBulkListener() {
        return new BulkProcessor.Listener() {

            private Instant startTime;

            public Instant getStartTime() {
                return startTime;
            }

            public void setStartTime(Instant startTime) {
                this.startTime = startTime;
            }

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                LOGGER.debug("Executing bulk [{}] with {} requests",
                        executionId, numberOfActions);
                setStartTime(Instant.now());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {
                if (response.hasFailures()) {
                    LOGGER.warn("Bulk [{}] executed with failures", executionId);
                    BulkItemResponse[] items = response.getItems();
                    for (BulkItemResponse responses : items) {
                        LOGGER.warn("Failure response message [{}]", responses.getFailureMessage());
                    }
                    statsDReporter.captureDurationSince(ES_SINK_PROCESSING_TIME, startTime, FAILURE_TAG);
                    statsDReporter.captureDurationSince(ES_SINK_FAILED_DOCUMENT_COUNT, getStartTime(), FAILURE_TAG);
                } else {
                    LOGGER.debug("Bulk [{}] completed in {} milliseconds",
                            executionId, response.getTook().getMillis());

                    System.out.printf("Bulk [%s] completed in %s milliseconds\n", executionId, response.getTook().getMillis());
                    statsDReporter.captureDurationSince(ES_SINK_PROCESSING_TIME, startTime, SUCCESS_TAG);
                    statsDReporter.captureDurationSince(ES_SINK_SUCCESS_DOCUMENT_COUNT, getStartTime(), SUCCESS_TAG);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  Throwable failure) {
                LOGGER.error("Failed to execute bulk", failure);
                failure.printStackTrace();
                statsDReporter.captureDurationSince(ES_SINK_PROCESSING_TIME, startTime, FAILURE_TAG);
                statsDReporter.captureDurationSince(ES_SINK_FAILED_DOCUMENT_COUNT, startTime, FAILURE_TAG);

            }
        };
    }

    public void processRequest(DocWriteRequest request) {
        getBulkProcessor().add(request);
    }

    public void close() {
        getBulkProcessor().close();
    }
}
//CHECKSTYLE:ON
