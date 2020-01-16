package com.gojek.esb.sink.elasticsearch;

import com.gojek.esb.config.enums.ESMessageType;
import com.gojek.esb.config.enums.ESRequestType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.serializer.EsbMessageToJson;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.nio.charset.Charset;
import java.util.Optional;

import static com.gojek.esb.config.enums.ESMessageType.PROTOBUF;


public class ESRequestBuilder {

    private final EsbMessageToJson jsonSerializer;
    private String esIdFieldName;
    private ESMessageType messageType;

    private ESRequestType esRequestType;
    private JSONParser jsonParser;

    public ESRequestBuilder(ESRequestType esRequestType, String esIdFieldName, ESMessageType messageType, EsbMessageToJson jsonSerializer) {
        this.jsonParser = new JSONParser();
        this.messageType = messageType;
        this.esRequestType = esRequestType;
        this.esIdFieldName = esIdFieldName;
        this.jsonSerializer = jsonSerializer;
    }

    public String extractId(EsbMessage message) {
        String payload = extractPayload(message);
        return getStringFromJson(payload, esIdFieldName);
    }

    DocWriteRequest buildRequest(String index, String type, EsbMessage message) {
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

    private String getStringFromJson(String payload, String fieldName) {
        try {
            JSONObject parse = (JSONObject) jsonParser.parse(payload);
            return Optional.of(parse.get(fieldName)).get().toString();
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        } finally {
            jsonParser.reset();
        }
    }

    private String extractPayload(EsbMessage message) {
        if (messageType.equals(PROTOBUF)) {
            return extractPayloadFromProtobuf(message);
        }
        return new String(message.getLogMessage(), Charset.defaultCharset());
    }

    private String extractPayloadFromProtobuf(EsbMessage message) {
        try {
            return getStringFromJson(jsonSerializer.serialize(message), "logMessage");
        } catch (DeserializerException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    private ESMessageType getMessageType() {
        return messageType;
    }
}
