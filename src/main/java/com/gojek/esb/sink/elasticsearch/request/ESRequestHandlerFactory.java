package com.gojek.esb.sink.elasticsearch.request;

import com.gojek.esb.config.ESSinkConfig;
import com.gojek.esb.config.enums.ESMessageType;
import com.gojek.esb.config.enums.ESRequestType;
import com.gojek.esb.serializer.EsbMessageToJson;

import java.util.ArrayList;

import static com.gojek.esb.config.enums.ESRequestType.INSERT_OR_UPDATE;
import static com.gojek.esb.config.enums.ESRequestType.UPDATE_ONLY;

public class ESRequestHandlerFactory {

    private ESSinkConfig esSinkConfig;
    private final String esIdFieldName;
    private final ESMessageType messageType;
    private final EsbMessageToJson jsonSerializer;
    private final String esTypeName;
    private final String esIndexName;

    public ESRequestHandlerFactory(ESSinkConfig esSinkConfig, String esIdFieldName, ESMessageType messageType,
                                   EsbMessageToJson jsonSerializer, String esTypeName, String esIndexName) {
        this.esSinkConfig = esSinkConfig;
        this.esIdFieldName = esIdFieldName;
        this.messageType = messageType;
        this.jsonSerializer = jsonSerializer;
        this.esTypeName = esTypeName;
        this.esIndexName = esIndexName;
    }

    public ESRequestHandler getRequestHandler() {
        ESRequestType esRequestType = esSinkConfig.isUpdateOnlyMode() ? UPDATE_ONLY : INSERT_OR_UPDATE;
        ArrayList<ESRequestHandler> esRequestHandlers = new ArrayList<>();
        esRequestHandlers.add(new ESUpdateRequestHandler(esRequestType, esIdFieldName, messageType, jsonSerializer, esTypeName, esIndexName));
        esRequestHandlers.add(new ESInsertRequestHandler(esRequestType, esIdFieldName, messageType, jsonSerializer, esTypeName, esIndexName));

        return esRequestHandlers
                .stream()
                .filter(esRequestHandler -> esRequestHandler.canCreate(esRequestType))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Es Request Type " + esRequestType.name() + " not supported"));
    }
}
