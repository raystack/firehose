package com.gojek.esb.sink.elasticsearch.request;

import com.gojek.esb.config.enums.ESMessageType;
import com.gojek.esb.config.enums.ESRequestType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.serializer.EsbMessageToJson;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class ESUpsertRequestHandler extends ESRequestHandler {
    private final String esTypeName;
    private final String esIndexName;
    private ESRequestType esRequestType;
    private String esIdFieldName;
    private String esRoutingKeyName;

    public ESUpsertRequestHandler(ESMessageType messageType, EsbMessageToJson jsonSerializer, String esTypeName, String esIndexName, ESRequestType esRequestType, String esIdFieldName, String esRoutingKeyName) {
        super(messageType, jsonSerializer);
        this.esTypeName = esTypeName;
        this.esIndexName = esIndexName;
        this.esRequestType = esRequestType;
        this.esIdFieldName = esIdFieldName;
        this.esRoutingKeyName = esRoutingKeyName;
    }

    @Override
    public boolean canCreate() {
        return esRequestType == ESRequestType.INSERT_OR_UPDATE;
    }

    public DocWriteRequest getRequest(EsbMessage esbMessage) {
        String esbLogMessage = extractPayload(esbMessage);
        IndexRequest request = new IndexRequest(esIndexName, esTypeName, getFieldFromJSON(esbLogMessage, esIdFieldName));
        if (StringUtils.isNotEmpty(esRoutingKeyName)) {
            request.routing(getFieldFromJSON(esbLogMessage, esRoutingKeyName));
        }
        request.source(esbLogMessage, XContentType.JSON);
        return request;
    }
}
