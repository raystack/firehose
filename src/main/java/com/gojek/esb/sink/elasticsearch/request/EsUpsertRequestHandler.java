package com.gojek.esb.sink.elasticsearch.request;

import com.gojek.esb.config.enums.EsSinkMessageType;
import com.gojek.esb.config.enums.EsSinkRequestType;
import com.gojek.esb.consumer.Message;
import com.gojek.esb.serializer.MessageToJson;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class EsUpsertRequestHandler extends EsRequestHandler {
    private final String esTypeName;
    private final String esIndexName;
    private EsSinkRequestType esSinkRequestType;
    private String esIdFieldName;
    private String esRoutingKeyName;

    public EsUpsertRequestHandler(EsSinkMessageType messageType, MessageToJson jsonSerializer, String esTypeName, String esIndexName, EsSinkRequestType esSinkRequestType, String esIdFieldName, String esRoutingKeyName) {
        super(messageType, jsonSerializer);
        this.esTypeName = esTypeName;
        this.esIndexName = esIndexName;
        this.esSinkRequestType = esSinkRequestType;
        this.esIdFieldName = esIdFieldName;
        this.esRoutingKeyName = esRoutingKeyName;
    }

    @Override
    public boolean canCreate() {
        return esSinkRequestType == EsSinkRequestType.INSERT_OR_UPDATE;
    }

    public DocWriteRequest getRequest(Message message) {
        String esbLogMessage = extractPayload(message);
        IndexRequest request = new IndexRequest(esIndexName, esTypeName, getFieldFromJSON(esbLogMessage, esIdFieldName));
        if (StringUtils.isNotEmpty(esRoutingKeyName)) {
            request.routing(getFieldFromJSON(esbLogMessage, esRoutingKeyName));
        }
        request.source(esbLogMessage, XContentType.JSON);
        return request;
    }
}
