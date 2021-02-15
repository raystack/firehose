package com.gojek.esb.sink.elasticsearch.request;

import com.gojek.esb.config.EsSinkConfig;
import com.gojek.esb.config.enums.EsSinkMessageType;
import com.gojek.esb.config.enums.EsSinkRequestType;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.serializer.MessageToJson;
import lombok.AllArgsConstructor;

import java.util.ArrayList;

import static com.gojek.esb.config.enums.EsSinkRequestType.INSERT_OR_UPDATE;
import static com.gojek.esb.config.enums.EsSinkRequestType.UPDATE_ONLY;

@AllArgsConstructor
public class EsRequestHandlerFactory {

    private EsSinkConfig esSinkConfig;
    private Instrumentation instrumentation;
    private final String esIdFieldName;
    private final EsSinkMessageType messageType;
    private final MessageToJson jsonSerializer;
    private final String esTypeName;
    private final String esIndexName;
    private final String esRoutingKeyName;

    public EsRequestHandler getRequestHandler() {
        EsSinkRequestType esSinkRequestType = esSinkConfig.isSinkEsModeUpdateOnlyEnable() ? UPDATE_ONLY : INSERT_OR_UPDATE;
        instrumentation.logInfo("ES request mode: {}", esSinkRequestType);

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
