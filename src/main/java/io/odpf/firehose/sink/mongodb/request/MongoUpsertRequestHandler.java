package io.odpf.firehose.sink.mongodb.request;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import io.odpf.firehose.config.enums.MongoSinkMessageType;
import io.odpf.firehose.config.enums.MongoSinkRequestType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.serializer.MessageToJson;
import org.bson.Document;

public class MongoUpsertRequestHandler extends MongoRequestHandler {

    private MongoSinkRequestType mongoSinkRequestType;
    private String mongoPrimaryKey;

    public MongoUpsertRequestHandler(MongoSinkMessageType messageType, MessageToJson jsonSerializer,  MongoSinkRequestType mongoSinkRequestType, String mongoPrimaryKey) {
        super(messageType, jsonSerializer);
        this.mongoSinkRequestType = mongoSinkRequestType;
        this.mongoPrimaryKey = mongoPrimaryKey;
    }

    @Override
    public boolean canCreate() {
        return mongoSinkRequestType == MongoSinkRequestType.INSERT_OR_UPDATE;
    }

    @Override
    public ReplaceOneModel<Document> getRequest(Message message) {
        String logMessage = extractPayload(message);
        Document document = Document.parse(logMessage);

        return new ReplaceOneModel<>(
                new Document(mongoPrimaryKey, getFieldFromJSON(logMessage, mongoPrimaryKey)),
                document,
        new ReplaceOptions().upsert(true) );
    }
}
