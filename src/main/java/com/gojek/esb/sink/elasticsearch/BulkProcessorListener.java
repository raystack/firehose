package com.gojek.esb.sink.elasticsearch;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.Instrumentation;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.ArrayList;
import java.util.List;

import static com.gojek.esb.metrics.Metrics.*;


public class BulkProcessorListener implements BulkProcessor.Listener {

    private Instrumentation instrumentation;
    private List<EsbMessage> esbMessages = new ArrayList<>();
    private List<EsbMessage> messageBulk;

    public BulkProcessorListener(Instrumentation instrumentation) {
        this.instrumentation = instrumentation;
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        int numberOfActions = request.numberOfActions();

        messageBulk = esbMessages.subList(0, request.numberOfActions());
        esbMessages = esbMessages.subList(request.numberOfActions(), esbMessages.size());

        instrumentation.capturePreExecutionLatencies(messageBulk);
        instrumentation.logDebug("Executing bulk [{}] with {} requests", executionId, numberOfActions);

        instrumentation.startExecution();
    }

    public void updateEsbMessages(List<EsbMessage> esbMessageList) {
        this.esbMessages.addAll(esbMessageList);
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        if (response.hasFailures()) {
            instrumentation.logWarn("Bulk [{}] executed with failures", executionId);
            instrumentation.logDebug("Failure message is [{}]", response.buildFailureMessage());

            BulkItemResponse[] items = response.getItems();
            int failedCount = 0;
            for (BulkItemResponse responses : items) {
                if (responses.isFailed()) {
                    failedCount += 1;
                    instrumentation.captureNonFatalError(new Exception(String.valueOf(responses.getFailure())));
                }
            }
            instrumentation.captureCountWithTags(MESSAGE_COUNT, failedCount, FAILURE_TAG);
            instrumentation.captureCountWithTags(MESSAGE_COUNT, (response.getItems().length - failedCount), SUCCESS_TAG);
        } else {
            instrumentation.logDebug("Bulk [{}] of {} messages completed in {} milliseconds", executionId, request.numberOfActions(), response.getTook().getMillis());
            instrumentation.captureSuccessExecutionTelemetry("elasticsearch", request.numberOfActions());
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        instrumentation.captureFailedExecutionTelemetry(new Exception(failure), request.numberOfActions());
    }

}
