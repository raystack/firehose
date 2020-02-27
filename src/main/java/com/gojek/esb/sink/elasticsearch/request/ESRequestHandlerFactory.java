package com.gojek.esb.sink.elasticsearch.request;

import com.gojek.esb.config.ESSinkConfig;
import com.gojek.esb.config.enums.ESMessageType;
import com.gojek.esb.config.enums.ESRequestType;
import com.gojek.esb.serializer.EsbMessageToJson;
import lombok.AllArgsConstructor;

import java.util.ArrayList;

import static com.gojek.esb.config.enums.ESRequestType.INSERT_OR_UPDATE;
import static com.gojek.esb.config.enums.ESRequestType.UPDATE_ONLY;

@AllArgsConstructor
public class ESRequestHandlerFactory {

    private ESSinkConfig esSinkConfig;
    private final String esIdFieldName;
    private final ESMessageType messageType;
    private final EsbMessageToJson jsonSerializer;
    private final String esTypeName;
    private final String esIndexName;

    public ESRequestHandler getRequestHandler() {
        ESRequestType esRequestType = esSinkConfig.isUpdateOnlyMode() ? UPDATE_ONLY : INSERT_OR_UPDATE;
        ArrayList<ESRequestHandler> esRequestHandlers = new ArrayList<>();
        esRequestHandlers.add(new ESUpdateRequestHandler(messageType, jsonSerializer, esTypeName, esIndexName, esRequestType, esIdFieldName));
        esRequestHandlers.add(new ESInsertRequestHandler(messageType, jsonSerializer, esTypeName, esIndexName, esRequestType, esIdFieldName));

        return esRequestHandlers
                .stream()
                .filter(ESRequestHandler::canCreate)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Es Request Type " + esRequestType.name() + " not supported"));
    }
}
