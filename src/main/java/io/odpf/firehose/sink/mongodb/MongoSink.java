package io.odpf.firehose.sink.mongodb;

import com.mongodb.BulkWriteError;
import com.mongodb.BulkWriteException;
import com.mongodb.MongoClient;
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
 * MongoDB sink
 */
public class MongoSink extends AbstractSink {
    private MongoRequestHandler mongoRequestHandler;
    private List<WriteModel<Document>> bulkRequest;
    private long mongoRequestTimeoutInMs;
    private List<String> mongoRetryStatusCodeBlacklist;
    private MongoCollection<Document> mongoCollection;
    private MongoClient mongoClient;

    /**
     * Instantiates a new Mongo sink.
     *
     * @param instrumentation         the instrumentation
     * @param sinkType                the sink type
     * @param mongoCollection         the client
     * @param mongoRequestHandler     the mongo request handler
     * @param mongoRequestTimeoutInMs the mongo request timeout in ms
     */
    public MongoSink(Instrumentation instrumentation, String sinkType, MongoCollection<Document> mongoCollection, MongoClient mongoClient, MongoRequestHandler mongoRequestHandler,
                     long mongoRequestTimeoutInMs, List<String> mongoRetryStatusCodeBlacklist) {
        super(instrumentation, sinkType);
        this.mongoCollection = mongoCollection;
        this.mongoRequestHandler = mongoRequestHandler;
        this.mongoClient = mongoClient;
        this.mongoRequestTimeoutInMs = mongoRequestTimeoutInMs;
        this.mongoRetryStatusCodeBlacklist=mongoRetryStatusCodeBlacklist;
    }

    @Override
    protected void prepare(List<Message> messages) {

        bulkRequest = new ArrayList<>();

        messages.forEach(message -> bulkRequest.add(mongoRequestHandler.getRequest(message)));
    }

    @Override
    protected List<Message> execute() throws Exception {
        List<BulkWriteError> bulkWriteErrors = getBulkWriteErrors();
        if (!bulkWriteErrors.isEmpty()) {
            getInstrumentation().logWarn("Bulk request failed");

            handleResponse(bulkWriteErrors);

        }

        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        getInstrumentation().logInfo("MongoDB connection closing");
        this.mongoClient.close();
    }

    List<BulkWriteError> getBulkWriteErrors() {
        List<BulkWriteError> bulkWriteErrors = new ArrayList<>();
        try {
            mongoCollection.bulkWrite(bulkRequest);
        } catch (BulkWriteException bulkWriteException) {

            bulkWriteErrors = bulkWriteException.getWriteErrors();
        }
        return bulkWriteErrors;
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
                throw new NeedToRetry(String.valueOf(bulkWriteError.getCode()));
            }

        }
        getInstrumentation().logWarn("Bulk request failed count: {}", failedResponseCount);
    }
}
