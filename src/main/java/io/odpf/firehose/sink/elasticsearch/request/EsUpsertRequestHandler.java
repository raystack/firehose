package io.odpf.firehose.sink.elasticsearch.request;

import io.odpf.firehose.config.enums.EsSinkMessageType;
import io.odpf.firehose.config.enums.EsSinkRequestType;
import io.odpf.firehose.type.Message;
import io.odpf.firehose.serializer.MessageToJson;
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
        String logMessage = extractPayload(message);
        IndexRequest request = new IndexRequest(esIndexName, esTypeName, getFieldFromJSON(logMessage, esIdFieldName));
        if (StringUtils.isNotEmpty(esRoutingKeyName)) {
            request.routing(getFieldFromJSON(logMessage, esRoutingKeyName));
        }
        request.source(logMessage, XContentType.JSON);
        return request;
    }
}
