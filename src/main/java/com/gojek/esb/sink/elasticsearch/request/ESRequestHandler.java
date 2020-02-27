package com.gojek.esb.sink.elasticsearch.request;

import com.gojek.esb.config.enums.ESMessageType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.JsonParseException;
import com.gojek.esb.serializer.EsbMessageToJson;
import org.elasticsearch.action.DocWriteRequest;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.nio.charset.Charset;

import static com.gojek.esb.config.enums.ESMessageType.PROTOBUF;

public abstract class ESRequestHandler {
    private final ESMessageType messageType;
    private final EsbMessageToJson jsonSerializer;
    private final JSONParser jsonParser;

    public ESRequestHandler(ESMessageType messageType, EsbMessageToJson jsonSerializer) {
        this.messageType = messageType;
        this.jsonSerializer = jsonSerializer;
        this.jsonParser = new JSONParser();
    }

    public abstract boolean canCreate();

    public abstract DocWriteRequest getRequest(EsbMessage esbMessage);

    String extractPayload(EsbMessage message) {
        if (messageType.equals(PROTOBUF)) {
            return getFieldFromJSON(jsonSerializer.serialize(message), "logMessage");
        }
        return new String(message.getLogMessage(), Charset.defaultCharset());
    }

    String getFieldFromJSON(String jsonString, String key) {
        try {
            JSONObject parse = (JSONObject) jsonParser.parse(jsonString);
            Object valueAtKey = parse.get(key);
            if (valueAtKey == null) {
                throw new IllegalArgumentException("Key: " + key + " not found in ESB Message");
            }
            return valueAtKey.toString();
        } catch (ParseException e) {
            throw new JsonParseException(e.getMessage(), e.getCause());
        } finally {
            jsonParser.reset();
        }
    }

}
