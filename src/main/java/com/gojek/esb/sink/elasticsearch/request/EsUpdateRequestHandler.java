package com.gojek.esb.sink.elasticsearch.request;

import com.gojek.esb.config.enums.EsSinkMessageType;
import com.gojek.esb.config.enums.EsSinkRequestType;
import com.gojek.esb.consumer.Message;
import com.gojek.esb.serializer.MessageToJson;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class EsUpdateRequestHandler extends EsRequestHandler {

    private final String esTypeName;
    private final String esIndexName;
    private EsSinkRequestType esSinkRequestType;
    private String esIdFieldName;
    private String esRoutingKeyName;

    public EsUpdateRequestHandler(EsSinkMessageType messageType, MessageToJson jsonSerializer, String esTypeName, String esIndexName, EsSinkRequestType esSinkRequestType, String esIdFieldName, String esRoutingKeyName) {
        super(messageType, jsonSerializer);
        this.esTypeName = esTypeName;
        this.esIndexName = esIndexName;
        this.esSinkRequestType = esSinkRequestType;
        this.esIdFieldName = esIdFieldName;
        this.esRoutingKeyName = esRoutingKeyName;
    }

    @Override
    public boolean canCreate() {
        return esSinkRequestType == EsSinkRequestType.UPDATE_ONLY;
    }

    public DocWriteRequest getRequest(Message message) {
        String esbLogMessage = extractPayload(message);
        UpdateRequest request = new UpdateRequest(esIndexName, esTypeName, getFieldFromJSON(esbLogMessage, esIdFieldName));
        if (StringUtils.isNotEmpty(esRoutingKeyName)) {
            request.routing(getFieldFromJSON(esbLogMessage, esRoutingKeyName));
        }
        request.doc(esbLogMessage, XContentType.JSON);
        return request;
    }
}
