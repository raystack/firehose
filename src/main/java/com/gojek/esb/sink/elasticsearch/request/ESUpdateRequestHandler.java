package com.gojek.esb.sink.elasticsearch.request;

import com.gojek.esb.config.enums.ESMessageType;
import com.gojek.esb.config.enums.ESRequestType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.serializer.EsbMessageToJson;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class ESUpdateRequestHandler extends ESRequestHandler {

    public ESUpdateRequestHandler(ESRequestType esRequestType, String esIdFieldName, ESMessageType messageType, EsbMessageToJson jsonSerializer, String esTypeName, String esIndexName) {
        super(esRequestType, esIdFieldName, messageType, jsonSerializer, esTypeName, esIndexName);
    }

    @Override
    public boolean canCreate(ESRequestType esRequestType) {
        return esRequestType == ESRequestType.UPDATE_ONLY;
    }

    public DocWriteRequest getRequest(EsbMessage esbMessage) {
        UpdateRequest request = new UpdateRequest(getEsIndexName(), getEsTypeName(), extractId(esbMessage));
        request.doc(extractPayload(esbMessage), XContentType.JSON);
        return request;
    }
}
