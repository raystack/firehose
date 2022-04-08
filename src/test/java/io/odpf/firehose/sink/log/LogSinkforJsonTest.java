package io.odpf.firehose.sink.log;

import io.odpf.firehose.config.AppConfig;
import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.parser.json.JsonMessageParser;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LogSinkforJsonTest {

    private Instrumentation instrumentation;
    private JsonMessageParser jsonMessageParser;


    @Before
    public void setUp() throws Exception {
        instrumentation = mock(Instrumentation.class);
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, Collections.emptyMap());
        this.jsonMessageParser = new JsonMessageParser(appConfig);
    }

    @Test
    public void shouldLogJsonString() throws Exception {
        String validJsonStr = "{ \"first_name\": \"john\" , \"last_name\": \"doe\"}";
        Message validMessage = createMessageWithLogMessage(validJsonStr);
        LogSinkforJson logSinkforJson = new LogSinkforJson(instrumentation, jsonMessageParser);
        logSinkforJson.prepare(asList(validMessage));
        List<Message> invalidMessages = logSinkforJson.execute();
        //no invalid messages
        assertTrue(invalidMessages.isEmpty());
        ArgumentCaptor<JSONObject> jsonObjectArgumentCaptor = ArgumentCaptor.forClass(JSONObject.class);
        verify(instrumentation, times(1)).logInfo(eq("\n================= DATA =======================\n{}"), jsonObjectArgumentCaptor.capture());

        /*
        JSONObject.equals does reference check, so cant use assertEquals
        reference https://github.com/stleary/JSON-java/blob/master/src/test/java/org/json/junit/JSONObjectTest.java#L132
         */
        JSONObject actualJSONObject = jsonObjectArgumentCaptor.getValue();
        assertTrue(new JSONObject(validJsonStr).similar(actualJSONObject));
    }

    @Test
    public void shouldReturnInvalidJsonMessages() throws Exception {
        LogSinkforJson logSinkforJson = new LogSinkforJson(instrumentation, jsonMessageParser);
        String validJsonStr = "{ \"first_name\": \"john\" , \"last_name\": \"doe\"}";
        Message validMessage = createMessageWithLogMessage(validJsonStr);
        Message invalidMessage = createMessageWithLogMessage("invalid\\ json");
        logSinkforJson.prepare(asList(validMessage, invalidMessage));
        List<Message> messageList = logSinkforJson.execute();
        assertEquals(1, messageList.size());

        Message actualInvalidMessage = messageList.get(0);
        assertEquals("invalid\\ json", new String(actualInvalidMessage.getLogMessage()));
        ErrorInfo actualError = actualInvalidMessage.getErrorInfo();
        assertNotNull(actualError);
        assertEquals(ErrorType.DESERIALIZATION_ERROR, actualError.getErrorType());
    }

    @Test
    public void shouldLogValidJsonMessagesIgnoringInvalidMessages() throws Exception {
        String validJsonStr = "{ \"first_name\": \"john\" , \"last_name\": \"doe\"}";
        String anotherValidJsonStr = "{ \"hello\": \"world\" }";
        String invalidJsonStr = "invalid\\ json";
        Message validMessage = createMessageWithLogMessage(validJsonStr);
        Message invalidMessage = createMessageWithLogMessage(invalidJsonStr);
        Message anotherValidMessage = createMessageWithLogMessage(anotherValidJsonStr);

        LogSinkforJson logSinkforJson = new LogSinkforJson(instrumentation, jsonMessageParser);
        logSinkforJson.prepare(asList(validMessage, invalidMessage, anotherValidMessage));
        logSinkforJson.execute();
        verify(instrumentation, times(2)).logInfo(eq("\n================= DATA =======================\n{}"), any(JSONObject.class));
    }

    private Message createMessageWithLogMessage(String s) {
        return new Message(null,
                s.getBytes(),
                "empty-topic",
                0, 0);
    }
}
