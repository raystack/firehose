package io.odpf.firehose.sink.mongodb;

import com.mongodb.BulkWriteError;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.WriteModel;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.mongodb.request.MongoRequestHandler;
import io.odpf.firehose.sink.mongodb.util.MongoResponseHandler;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
     * @param instrumentation               the instrumentation
     * @param sinkType                      the sink type
     * @param mongoCollection               the mongo collection
     * @param mongoClient                   the mongo client
     * @param mongoRequestHandler           the mongo request handler
     * @param mongoRetryStatusCodeBlacklist the mongo retry status code blacklist
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

        if (bulkWriteErrors.isEmpty()) {
            return Collections.emptyList();
        }
        getInstrumentation().logWarn("Bulk request failed");
        handleBulkWriteErrors(bulkWriteErrors);

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

    protected  List<BulkWriteError> processRequest(){
        return MongoResponseHandler.processRequest(bulkRequest, mongoCollection,getInstrumentation());
    }

    private void handleBulkWriteErrors(List<BulkWriteError> bulkWriteErrors) {
        int failedResponseCount = 0;
        for (BulkWriteError bulkWriteError : bulkWriteErrors) {
            failedResponseCount++;
            String responseStatus = String.valueOf(bulkWriteError.getCode());
            if (mongoRetryStatusCodeBlacklist.contains(responseStatus)) {
                getInstrumentation().logInfo("Not retrying due to response status: {} is under blacklisted status code", responseStatus);
                getInstrumentation().incrementCounterWithTags(SINK_MESSAGES_DROP_TOTAL, "cause=" + bulkWriteError.getMessage());
                getInstrumentation().logInfo("Message dropped because of status code: " + responseStatus);
            }
        }
        getInstrumentation().logWarn("Bulk request failed count: {}", failedResponseCount);
    }
}
