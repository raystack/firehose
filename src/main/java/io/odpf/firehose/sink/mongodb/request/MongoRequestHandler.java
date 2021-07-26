package io.odpf.firehose.sink.mongodb.request;


import com.mongodb.client.model.WriteModel;
import io.odpf.firehose.config.enums.MongoSinkMessageType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.JsonParseException;
import io.odpf.firehose.serializer.MessageToJson;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.nio.charset.Charset;

/**
 * The type Mongo request handler.
 */
public abstract class MongoRequestHandler {
    private final MongoSinkMessageType messageType;
    private final MessageToJson jsonSerializer;
    private final JSONParser jsonParser;

    /**
     * Instantiates a new Mongo request handler.
     *
     * @param messageType    the message type
     * @param jsonSerializer the json serializer
     */
    public MongoRequestHandler(MongoSinkMessageType messageType, MessageToJson jsonSerializer) {
        this.messageType = messageType;
        this.jsonSerializer = jsonSerializer;
        this.jsonParser = new JSONParser();
    }

    /**
     * Can create the specified request type or not.
     *
     * @return true if the specified request type can be created otherwise false
     */
    public abstract boolean canCreate();

    /**
     * Gets request.
     *
     * @param message the message
     * @return the request
     */
    public abstract WriteModel<Document> getRequest(Message message);

    /**
     * Extract payload string.
     *
     * @param message the message
     * @return the JSON string parsed from the message
     */
    protected String extractPayload(Message message) {
        if (messageType.equals(MongoSinkMessageType.PROTOBUF)) {
            return getFieldFromJSON(jsonSerializer.serialize(message), "logMessage");
        }
        return new String(message.getLogMessage(), Charset.defaultCharset());
    }

    /**
     * Gets field from json.
     *
     * @param jsonString the json string
     * @param key        the key
     * @return the field from json
     */
    protected String getFieldFromJSON(String jsonString, String key) {
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
