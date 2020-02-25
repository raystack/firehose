package com.gojek.esb.sink.elasticsearch.request;

import com.gojek.esb.config.enums.ESMessageType;
import com.gojek.esb.config.enums.ESRequestType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.serializer.EsbMessageToJson;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class ESInsertRequestHandler extends ESRequestHandler {
    public ESInsertRequestHandler(ESRequestType esRequestType, String esIdFieldName, ESMessageType messageType, EsbMessageToJson jsonSerializer, String esTypeName, String esIndexName) {
        super(esRequestType, esIdFieldName, messageType, jsonSerializer, esTypeName, esIndexName);
    }

    @Override
    public boolean canCreate(ESRequestType esRequestType) {
        return esRequestType == ESRequestType.INSERT_OR_UPDATE;
    }

    public DocWriteRequest getRequest(EsbMessage esbMessage) {
        IndexRequest request = new IndexRequest(getEsIndexName(), getEsTypeName(), extractId(esbMessage));
        request.source(extractPayload(esbMessage), getMessageType().equals(ESMessageType.JSON) ? XContentType.JSON : XContentType.JSON);
        return request;
    }
}
