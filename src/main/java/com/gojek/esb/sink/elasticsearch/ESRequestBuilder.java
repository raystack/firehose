package com.gojek.esb.sink.elasticsearch;

import com.gojek.esb.consumer.EsbMessage;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.nio.charset.Charset;
import java.util.Optional;

public class ESRequestBuilder {

    private String esIdFieldName;
    private ESMessageType messageType;

    private ESRequestType esRequestType;
    private JSONParser jsonParser;

    public ESRequestBuilder(ESRequestType esRequestType, String esIdFieldName) {
        messageType = ESMessageType.JSON;
        jsonParser = new JSONParser();
        this.esRequestType = esRequestType;
        this.esIdFieldName = esIdFieldName;
    }

    public DocWriteRequest buildRequest(String index, String type, EsbMessage message) {
        if (getEsRequestType() == ESRequestType.UPDATE_ONLY) {
            return buildUpdateRequest(index, type, extractId(message), extractPayload(message));
        }
        return buildInsertRequest(index, type, extractId(message), extractPayload(message));
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

        try {
            JSONObject parse = (JSONObject) jsonParser.parse(payload);
            return (String) Optional.of(parse.get(esIdFieldName)).get();
        } catch (ParseException e) {
            throw new RuntimeException();
        } finally {
            jsonParser.reset();
        }
    }

    String extractPayload(EsbMessage message) {
        return new String(message.getLogMessage(), Charset.defaultCharset());
    }

    private ESMessageType getMessageType() {
        return messageType;
    }
}
