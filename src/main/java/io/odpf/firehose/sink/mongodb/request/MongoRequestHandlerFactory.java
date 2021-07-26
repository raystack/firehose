package io.odpf.firehose.sink.mongodb.request;

import io.odpf.firehose.config.MongoSinkConfig;
import io.odpf.firehose.config.enums.MongoSinkMessageType;
import io.odpf.firehose.config.enums.MongoSinkRequestType;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.serializer.MessageToJson;
import lombok.AllArgsConstructor;

import java.util.ArrayList;

import static io.odpf.firehose.config.enums.MongoSinkRequestType.INSERT_OR_UPDATE;
import static io.odpf.firehose.config.enums.MongoSinkRequestType.UPDATE_ONLY;

@AllArgsConstructor
public class MongoRequestHandlerFactory {

    private final MongoSinkConfig mongoSinkConfig;
    private final Instrumentation instrumentation;
    private final String mongoPrimaryKey;
    private final MongoSinkMessageType messageType;
    private final MessageToJson jsonSerializer;


    public MongoRequestHandler getRequestHandler() {
        MongoSinkRequestType mongoSinkRequestType = mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable() ? UPDATE_ONLY : INSERT_OR_UPDATE;
        instrumentation.logInfo("Mongo request mode: {}", mongoSinkRequestType);

        ArrayList<MongoRequestHandler> mongoRequestHandlers = new ArrayList<>();
        mongoRequestHandlers.add(new MongoUpdateRequestHandler(messageType, jsonSerializer, mongoSinkRequestType, mongoPrimaryKey));
        mongoRequestHandlers.add(new MongoUpsertRequestHandler(messageType, jsonSerializer, mongoSinkRequestType, mongoPrimaryKey));

        return mongoRequestHandlers
                .stream()
                .filter(MongoRequestHandler::canCreate)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Mongo Request Type " + mongoSinkRequestType.name() + " not supported"));
    }
}
