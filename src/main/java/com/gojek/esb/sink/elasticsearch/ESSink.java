package com.gojek.esb.sink.elasticsearch;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.NeedToRetry;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.elasticsearch.request.ESRequestHandler;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ESSink extends AbstractSink {
    private RestHighLevelClient client;
    private ESRequestHandler esRequestHandler;
    private BulkRequest bulkRequest;
    private long esRequestTimeoutInMs;
    private Integer esWaitForActiveShardsCount;

    public ESSink(Instrumentation instrumentation, String sinkType, RestHighLevelClient client, ESRequestHandler esRequestHandler,
                  long esRequestTimeoutInMs, Integer esWaitForActiveShardsCount) {
        super(instrumentation, sinkType);
        this.client = client;
        this.esRequestHandler = esRequestHandler;
        this.esRequestTimeoutInMs = esRequestTimeoutInMs;
        this.esWaitForActiveShardsCount = esWaitForActiveShardsCount;
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
            getInstrumentation().logInfo("Bulk request failed");
            throw new NeedToRetry(bulkResponse.buildFailureMessage());
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        this.client.close();
    }

    BulkResponse getBulkResponse() throws IOException {
        return client.bulk(bulkRequest);
    }
}
