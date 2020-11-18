package com.gojek.esb.sink.elasticsearch;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.NeedToRetry;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.elasticsearch.request.ESRequestHandler;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.gojek.esb.metrics.Metrics.MESSAGES_DROPPED_COUNT;

public class ESSink extends AbstractSink {
    private RestHighLevelClient client;
    private ESRequestHandler esRequestHandler;
    private BulkRequest bulkRequest;
    private long esRequestTimeoutInMs;
    private Integer esWaitForActiveShardsCount;
    private List<String> esRetryStatusCodeBlacklist;

    public ESSink(Instrumentation instrumentation, String sinkType, RestHighLevelClient client, ESRequestHandler esRequestHandler,
                  long esRequestTimeoutInMs, Integer esWaitForActiveShardsCount, List<String> esRetryStatusCodeBlacklist) {
        super(instrumentation, sinkType);
        this.client = client;
        this.esRequestHandler = esRequestHandler;
        this.esRequestTimeoutInMs = esRequestTimeoutInMs;
        this.esWaitForActiveShardsCount = esWaitForActiveShardsCount;
        this.esRetryStatusCodeBlacklist = esRetryStatusCodeBlacklist;
    }

    @Override
    protected void prepare(List<EsbMessage> esbMessages) {
        bulkRequest = new BulkRequest();
        bulkRequest.timeout(TimeValue.timeValueMillis(esRequestTimeoutInMs));
        bulkRequest.waitForActiveShards(esWaitForActiveShardsCount);
        esbMessages.forEach(esbMessage -> bulkRequest.add(esRequestHandler.getRequest(esbMessage)));
    }

    @Override
    protected List<EsbMessage> execute() throws Exception {
        BulkResponse bulkResponse = getBulkResponse();
        if (bulkResponse.hasFailures()) {
            getInstrumentation().logWarn("Bulk request failed");
            handleResponse(bulkResponse);
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        getInstrumentation().logInfo("Elastic Search connection closing");
        this.client.close();
    }

    BulkResponse getBulkResponse() throws IOException {
        return client.bulk(bulkRequest);
    }

    private void handleResponse(BulkResponse bulkResponse) throws NeedToRetry {
        int failedResponseCount = 0;
        for (BulkItemResponse response : bulkResponse.getItems()) {
            if (response.isFailed()) {
                failedResponseCount++;
                String responseStatus = String.valueOf(response.status().getStatus());
                if (esRetryStatusCodeBlacklist.contains(responseStatus)) {
                    getInstrumentation().logInfo("Not retrying due to response status: {} is under blacklisted status code", responseStatus);
                    getInstrumentation().incrementCounterWithTags(MESSAGES_DROPPED_COUNT, "cause=" + response.status().name());
                    getInstrumentation().logInfo("Message dropped because of status code: " + responseStatus);
                } else {
                    throw new NeedToRetry(bulkResponse.buildFailureMessage());
                }
            }
        }
        getInstrumentation().logWarn("Bulk request failed count: {}", failedResponseCount);
    }
}
