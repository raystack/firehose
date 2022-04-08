package io.odpf.firehose.parser.json;

import io.odpf.firehose.config.AppConfig;
import io.odpf.firehose.message.Message;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class JsonMessageParserTest {
    /*
        JSONObject.equals does reference check, so cant use assertEquals
        reference https://github.com/stleary/JSON-java/blob/master/src/test/java/org/json/junit/JSONObjectTest.java#L132
     */
    @Test
    public void shouldParseJson() {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, Collections.emptyMap());
        JsonMessageParser jsonMessageParser = new JsonMessageParser(appConfig);
        String validJsonStr = "{ \"first_name\": \"john\", \"last_name\": \"doe\"}";
        Message m = createMessageWithLogMessage(validJsonStr);
        JSONObject actualJSONObject = jsonMessageParser.parse(m);
        JSONObject expectedJSON = new JSONObject(validJsonStr);
        assertTrue(expectedJSON.similar(actualJSONObject));
    }

    @Test
    public void shouldThrowExceptionForInvalidJson() {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, Collections.emptyMap());
        JsonMessageParser jsonMessageParser = new JsonMessageParser(appConfig);
        String invalidJsonStr = "{ \"first_name\": ";
        Message m = createMessageWithLogMessage(invalidJsonStr);
        assertThrows(JSONException.class, () -> jsonMessageParser.parse(m));
    }

    @Test
    public void shouldParseKey() {
        HashMap<String, String> configMap = new HashMap<>();
        configMap.put("KAFKA_RECORD_PARSER_MODE", "key");
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, configMap);
        JsonMessageParser jsonMessageParser = new JsonMessageParser(appConfig);
        String validJsonkey = "{ \"first_name\": \"john\", \"last_name\": \"doe\"}";
        String validJsonMessage = "{ \"gameofthrones\": \"season1\" }";
        Message messageWithKey = new Message(validJsonkey.getBytes(), validJsonMessage.getBytes(), "empty-topic", 0, 0);

        JSONObject actualJSON = jsonMessageParser.parse(messageWithKey);
        JSONObject expectedJSON = new JSONObject(validJsonkey);
        assertTrue(expectedJSON.similar(actualJSON));
    }


    @Test
    public void shouldParseMessage() {
        HashMap<String, String> configMap = new HashMap<>();
        configMap.put("KAFKA_RECORD_PARSER_MODE", "message");
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, configMap);
        JsonMessageParser jsonMessageParser = new JsonMessageParser(appConfig);
        String validJsonkey = "{ \"first_name\": \"john\", \"last_name\": \"doe\"}";
        String validJsonMessage = "{ \"gameofthrones\": \"season1\" }";
        Message messageWithKey = new Message(validJsonkey.getBytes(), validJsonMessage.getBytes(), "empty-topic", 0, 0);

        JSONObject actualJSON = jsonMessageParser.parse(messageWithKey);
        JSONObject expectedJSON = new JSONObject(validJsonMessage);
        assertTrue(expectedJSON.similar(actualJSON));
    }

    private Message createMessageWithLogMessage(String s) {
        return new Message(null,
                s.getBytes(),
                "empty-topic",
                0, 0);
    }

}
