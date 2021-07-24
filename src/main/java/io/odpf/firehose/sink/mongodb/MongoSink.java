package io.odpf.firehose.sink.mongodb;

import com.mongodb.BulkWriteError;
import com.mongodb.BulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.WriteModel;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.NeedToRetry;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.mongodb.request.MongoRequestHandler;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.odpf.firehose.metrics.Metrics.SINK_MESSAGES_DROP_TOTAL;

/**
 * Elastic search sink.
 */
public class MongoSink extends AbstractSink {
    private MongoRequestHandler mongoRequestHandler;
    private List<WriteModel<Document>> bulkRequest;
    private long mongoRequestTimeoutInMs;
    private List<String> mongoRetryStatusCodeBlacklist;
    private MongoCollection<Document> mongoCollection;
    private MongoClient mongoClient;

    /**
     * Instantiates a new Es sink.
     *
     * @param instrumentation            the instrumentation
     * @param sinkType                   the sink type
     * @param mongoCollection                     the client
     * @param mongoRequestHandler        the es request handler
     * @param mongoRequestTimeoutInMs       the es request timeout in ms
     */
    public MongoSink(Instrumentation instrumentation, String sinkType, MongoCollection<Document> mongoCollection,MongoClient mongoClient, MongoRequestHandler mongoRequestHandler,
                     long mongoRequestTimeoutInMs) {
        super(instrumentation, sinkType);
        this.mongoCollection = mongoCollection;
        this.mongoRequestHandler = mongoRequestHandler;
        this.mongoClient=mongoClient;
        this.mongoRequestTimeoutInMs = mongoRequestTimeoutInMs;
      }

    @Override
    protected void prepare(List<Message> messages) {

        bulkRequest = new ArrayList<>();

        messages.forEach(message -> bulkRequest.add(mongoRequestHandler.getRequest(message)));
    }

    @Override
    protected List<Message> execute() throws Exception {
        BulkWriteResult bulkResponse = null;
        try {
            bulkResponse = getBulkResponse();
        } catch (BulkWriteException bulkWriteException) {
            getInstrumentation().logWarn("Bulk request failed");

            handleResponse(bulkWriteException.getWriteErrors());

        }

        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        getInstrumentation().logInfo("Elastic Search connection closing");
        this.mongoClient.close();
    }

    BulkWriteResult getBulkResponse() {
        return mongoCollection.bulkWrite(bulkRequest);

    }

    private void handleResponse(List<BulkWriteError> bulkResponse) throws NeedToRetry {
        int failedResponseCount = 0;
        for (BulkWriteError bulkWriteError : bulkResponse) {
            failedResponseCount++;
            String responseStatus = String.valueOf(bulkWriteError.getCode());
            if (mongoRetryStatusCodeBlacklist.contains(responseStatus)) {
                getInstrumentation().logInfo("Not retrying due to response status: {} is under blacklisted status code", responseStatus);
                getInstrumentation().incrementCounterWithTags(SINK_MESSAGES_DROP_TOTAL, "cause=" + bulkWriteError.getMessage());
                getInstrumentation().logInfo("Message dropped because of status code: " + responseStatus);
            } else {
                throw new NeedToRetry(bulkWriteError.toString());
            }

        }
        getInstrumentation().logWarn("Bulk request failed count: {}", failedResponseCount);
    }
}
