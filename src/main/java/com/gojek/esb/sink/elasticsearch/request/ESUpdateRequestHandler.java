package com.gojek.esb.sink.elasticsearch.request;

import com.gojek.esb.config.enums.ESMessageType;
import com.gojek.esb.config.enums.ESRequestType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.serializer.EsbMessageToJson;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class ESUpdateRequestHandler extends ESRequestHandler {

    private final String esTypeName;
    private final String esIndexName;
    private ESRequestType esRequestType;
    private String esIdFieldName;

    public ESUpdateRequestHandler(ESMessageType messageType, EsbMessageToJson jsonSerializer, String esTypeName, String esIndexName, ESRequestType esRequestType, String esIdFieldName) {
        super(messageType, jsonSerializer);
        this.esTypeName = esTypeName;
        this.esIndexName = esIndexName;
        this.esRequestType = esRequestType;
        this.esIdFieldName = esIdFieldName;
    }

    @Override
    public boolean canCreate() {
        return esRequestType == ESRequestType.UPDATE_ONLY;
    }

    public DocWriteRequest getRequest(EsbMessage esbMessage) {
        String esbLogMessage = extractPayload(esbMessage);
        UpdateRequest request = new UpdateRequest(esIndexName, esTypeName, getFieldFromJSON(esbLogMessage, esIdFieldName));
        request.doc(esbLogMessage, XContentType.JSON);
        return request;
    }
}
