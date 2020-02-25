package com.gojek.esb.sink.elasticsearch.request;

import com.gojek.esb.config.enums.ESMessageType;
import com.gojek.esb.config.enums.ESRequestType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.serializer.EsbMessageToJson;
import lombok.Getter;
import org.elasticsearch.action.DocWriteRequest;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.nio.charset.Charset;
import java.util.Optional;

import static com.gojek.esb.config.enums.ESMessageType.PROTOBUF;

@Getter
public abstract class ESRequestHandler {
    private final ESRequestType esRequestType;
    private final String esIdFieldName;
    private final ESMessageType messageType;
    private final EsbMessageToJson jsonSerializer;
    private final String esTypeName;
    private final String esIndexName;
    private final JSONParser jsonParser;

    public ESRequestHandler(ESRequestType esRequestType, String esIdFieldName, ESMessageType messageType, EsbMessageToJson jsonSerializer, String esTypeName, String esIndexName) {
        this.esRequestType = esRequestType;
        this.esIdFieldName = esIdFieldName;
        this.messageType = messageType;
        this.jsonSerializer = jsonSerializer;
        this.esTypeName = esTypeName;
        this.esIndexName = esIndexName;
        this.jsonParser = new JSONParser();
    }

    public abstract boolean canCreate(ESRequestType requestType);

    public abstract DocWriteRequest getRequest(EsbMessage esbMessage);

    protected String extractId(EsbMessage message) {
        String payload = extractPayload(message);
        return getStringFromJson(payload, esIdFieldName);
    }

    protected String extractPayload(EsbMessage message) {
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
}
