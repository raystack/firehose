package io.odpf.firehose.sink.mongodb.request;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import io.odpf.firehose.config.enums.MongoSinkMessageType;
import io.odpf.firehose.config.enums.MongoSinkRequestType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.serializer.MessageToJson;
import org.bson.Document;

/**
 * Mongo request handler for upsert operation.
 */
public class MongoUpsertRequestHandler extends MongoRequestHandler {

    private final MongoSinkRequestType mongoSinkRequestType;
    private final String mongoPrimaryKey;

    /**
     * Instantiates a new Mongo upsert request handler.
     *
     * @param messageType          the message type
     * @param jsonSerializer       the json serializer
     * @param mongoSinkRequestType the mongo sink request type
     * @param mongoPrimaryKey      the mongo primary key
     */
    public MongoUpsertRequestHandler(MongoSinkMessageType messageType, MessageToJson jsonSerializer, MongoSinkRequestType mongoSinkRequestType, String mongoPrimaryKey) {
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
        document.append("_id",mongoPrimaryKey);

        return new ReplaceOneModel<>(
                new Document("_id", getFieldFromJSON(logMessage, mongoPrimaryKey)),
                document,
                new ReplaceOptions().upsert(true));
    }
}
