package com.gojek.esb.sink.elasticsearch;

import com.gojek.esb.consumer.EsbMessage;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.nio.charset.Charset;

public class ESRequestBuilder {

    private ESMessageType messageType;

    private ESRequestType esRequestType;

    public ESRequestBuilder(ESRequestType esRequestType) {
        messageType = ESMessageType.JSON;
        this.esRequestType = esRequestType;
    }

    public DocWriteRequest buildRequest(String index, String type, String id, String payload) {
        switch (getEsRequestType()) {
            case UPDATE_ONLY:
                return buildUpdateRequest(index, type, id, payload);
            default:
                return buildInsertRequest(index, type, id, payload);
        }
    }

    private DocWriteRequest buildUpdateRequest(String index, String type, String id, String payload) {
        UpdateRequest request = new UpdateRequest(index, type, id);
        request.doc(payload, getMessageType().equals(ESMessageType.JSON) ? XContentType.JSON : XContentType.JSON);
        return request;
    }

    private DocWriteRequest buildInsertRequest(String index, String type, String id, String payload) {
        IndexRequest request = new IndexRequest(index, type, id);
        request.source(payload, getMessageType().equals(ESMessageType.JSON) ? XContentType.JSON : XContentType.JSON);
        return request;
    }

    private ESRequestType getEsRequestType() {
        return esRequestType;
    }

    public String extractId(EsbMessage message) {
        String payload = extractPayload(message);
        final int i = 4;
        return payload.substring(payload.indexOf("\"customer_id\"") + "customer_id".length() + i, payload.indexOf("\","));
    }

    String extractPayload(EsbMessage message) {
        return new String(message.getLogMessage(), Charset.defaultCharset());
    }

    private ESMessageType getMessageType() {
        return messageType;
    }
}
