package io.odpf.firehose.sink.elasticsearch.request;

import io.odpf.firehose.config.EsSinkConfig;
import io.odpf.firehose.config.enums.EsSinkMessageType;
import io.odpf.firehose.config.enums.EsSinkRequestType;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.serializer.MessageToJson;
import lombok.AllArgsConstructor;

import java.util.ArrayList;

import static io.odpf.firehose.config.enums.EsSinkRequestType.INSERT_OR_UPDATE;
import static io.odpf.firehose.config.enums.EsSinkRequestType.UPDATE_ONLY;

@AllArgsConstructor
public class EsRequestHandlerFactory {

    private EsSinkConfig esSinkConfig;
    private FirehoseInstrumentation firehoseInstrumentation;
    private final String esIdFieldName;
    private final EsSinkMessageType messageType;
    private final MessageToJson jsonSerializer;
    private final String esTypeName;
    private final String esIndexName;
    private final String esRoutingKeyName;

    public EsRequestHandler getRequestHandler() {
        EsSinkRequestType esSinkRequestType = esSinkConfig.isSinkEsModeUpdateOnlyEnable() ? UPDATE_ONLY : INSERT_OR_UPDATE;
        firehoseInstrumentation.logInfo("ES request mode: {}", esSinkRequestType);

        ArrayList<EsRequestHandler> esRequestHandlers = new ArrayList<>();
        esRequestHandlers.add(new EsUpdateRequestHandler(messageType, jsonSerializer, esTypeName, esIndexName, esSinkRequestType, esIdFieldName, esRoutingKeyName));
        esRequestHandlers.add(new EsUpsertRequestHandler(messageType, jsonSerializer, esTypeName, esIndexName, esSinkRequestType, esIdFieldName, esRoutingKeyName));

        return esRequestHandlers
                .stream()
                .filter(EsRequestHandler::canCreate)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Es Request Type " + esSinkRequestType.name() + " not supported"));
    }
}
