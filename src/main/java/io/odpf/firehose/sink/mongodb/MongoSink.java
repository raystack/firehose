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
 * MongoDB sink.
 */
public class MongoSink extends AbstractSink {

    private final MongoRequestHandler mongoRequestHandler;
    private final List<WriteModel<Document>> bulkRequest = new ArrayList<>();
    private final List<String> mongoRetryStatusCodeBlacklist;
    private final MongoCollection<Document> mongoCollection;
    private final MongoClient mongoClient;
    private List<Message> messages;

    /**
     * Instantiates a new Mongo sink.
     *
     * @param instrumentation     the instrumentation
     * @param sinkType            the sink type
     * @param mongoCollection     the client
     * @param mongoRequestHandler the mongo request handler
     */
    public MongoSink(Instrumentation instrumentation, String sinkType, MongoCollection<Document> mongoCollection, MongoClient mongoClient, MongoRequestHandler mongoRequestHandler,
                     List<String> mongoRetryStatusCodeBlacklist) {
        super(instrumentation, sinkType);
        this.mongoCollection = mongoCollection;
        this.mongoRequestHandler = mongoRequestHandler;
        this.mongoClient = mongoClient;
        this.mongoRetryStatusCodeBlacklist = mongoRetryStatusCodeBlacklist;
    }

    @Override
    protected void prepare(List<Message> messageList) {
        this.messages = messageList;
        bulkRequest.clear();

        messages.forEach(message -> bulkRequest.add(mongoRequestHandler.getRequest(message)));
    }

    @Override
    protected List<Message> execute() throws Exception {
        List<BulkWriteError> bulkWriteErrors = processRequest();
        if (!bulkWriteErrors.isEmpty()) {
            getInstrumentation().logWarn("Bulk request failed");

            handleBulkWriteErrors(bulkWriteErrors);
        }

        List<Message> failedMessages = new ArrayList<>();

        bulkWriteErrors.forEach((bulkWriteError) -> {
            if (!mongoRetryStatusCodeBlacklist.contains(String.valueOf(bulkWriteError.getCode()))) {
                failedMessages.add(messages.get(bulkWriteError.getIndex()));
            }
        });
        return failedMessages;
    }

    @Override
    public void close() throws IOException {
        getInstrumentation().logInfo("MongoDB connection closing");
        this.mongoClient.close();
    }

    List<BulkWriteError> processRequest() {
        List<BulkWriteError> bulkWriteErrors = new ArrayList<>();
        try {
            handleBulkWriteResult(mongoCollection.bulkWrite(bulkRequest));
        } catch (BulkWriteException bulkWriteException) {

            bulkWriteErrors = bulkWriteException.getWriteErrors();
        }

        return bulkWriteErrors;
    }

    private void handleBulkWriteResult(BulkWriteResult bulkWriteResult) {

        getInstrumentation().logInfo("Successfully inserted {} documents", bulkWriteResult.getInsertedCount());
        getInstrumentation().logInfo("Successfully updated {} documents", bulkWriteResult.getModifiedCount());

    }


    private void handleBulkWriteErrors(List<BulkWriteError> bulkWriteErrors) throws NeedToRetry {
        int failedResponseCount = 0;
        for (BulkWriteError bulkWriteError : bulkWriteErrors) {
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
