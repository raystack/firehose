package org.raystack.firehose.sink.elasticsearch.request;

import org.raystack.firehose.config.enums.EsSinkMessageType;
import org.raystack.firehose.config.enums.EsSinkRequestType;
import org.raystack.firehose.message.Message;
import org.raystack.firehose.serializer.MessageToJson;
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
        String logMessage = extractPayload(message);
        UpdateRequest request = new UpdateRequest(esIndexName, esTypeName, getFieldFromJSON(logMessage, esIdFieldName));
        if (StringUtils.isNotEmpty(esRoutingKeyName)) {
            request.routing(getFieldFromJSON(logMessage, esRoutingKeyName));
        }
        request.doc(logMessage, XContentType.JSON);
        return request;
    }
}
