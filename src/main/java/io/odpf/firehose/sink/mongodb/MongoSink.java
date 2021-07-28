package io.odpf.firehose.sink.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.model.WriteModel;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.mongodb.request.MongoRequestHandler;
import io.odpf.firehose.sink.mongodb.response.MongoResponseHandler;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * MongoDB sink.
 */
public class MongoSink extends AbstractSink {

    private final MongoRequestHandler mongoRequestHandler;
    private final List<WriteModel<Document>> request = new ArrayList<>();
    private final MongoResponseHandler mongoResponseHandler;
    private final MongoClient mongoClient;
    private List<Message> messages;

    /**
     * Instantiates a new Mongo sink.
     *
     * @param instrumentation               the instrumentation
     * @param sinkType                      the sink type
     * @param mongoClient                   the mongo client
     * @param mongoRequestHandler           the mongo request handler
     */
    public MongoSink(Instrumentation instrumentation, String sinkType, MongoClient mongoClient, MongoRequestHandler mongoRequestHandler,
                     MongoResponseHandler mongoResponseHandler) {
        super(instrumentation, sinkType);
        this.mongoRequestHandler = mongoRequestHandler;
        this.mongoClient = mongoClient;
        this.mongoResponseHandler = mongoResponseHandler;
    }

    @Override
    protected void prepare(List<Message> messageList) {
        this.messages = messageList;
        request.clear();
        messages.forEach(message -> request.add(mongoRequestHandler.getRequest(message)));
    }

    @Override
    protected List<Message> execute() throws Exception {
        List<BulkWriteError> writeErrors = mongoResponseHandler.processRequest(request);

        if (writeErrors.isEmpty()) {
            return Collections.emptyList();
        }
        return writeErrors.stream()
                .map(writeError -> messages.get(writeError.getIndex()))
                .collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException {
        getInstrumentation().logInfo("MongoDB connection closing");
        this.mongoClient.close();
    }
}
