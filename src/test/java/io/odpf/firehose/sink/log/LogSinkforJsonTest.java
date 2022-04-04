package io.odpf.firehose.sink.log;

import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.Instrumentation;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogSinkforJsonTest {

    @Test
    public void shouldLogJsonString() throws Exception {
        Instrumentation instrumentation = null;
        LogSinkforJson logSinkforJson = new LogSinkforJson(instrumentation);
        logSinkforJson.prepare(Collections.emptyList());
        List<Message> messageList = logSinkforJson.execute();
        assertTrue(messageList.isEmpty());
    }

    @Test
    public void shouldReturnInvalidJsonMessages() throws Exception {
        Instrumentation instrumentation = null;
        LogSinkforJson logSinkforJson = new LogSinkforJson(instrumentation);
        Message validMessage = createMessageWithLogMessage("{ \"first_name\": \"john\" , \"last_name\": \"doe\"}");
        Message invalidMessage = createMessageWithLogMessage("hello world");
        logSinkforJson.prepare(Arrays.asList(validMessage, invalidMessage));
        List<Message> messageList = logSinkforJson.execute();
        assertEquals(1 , messageList.size());
        assertEquals(invalidMessage, messageList.get(0));
    }

    private Message createMessageWithLogMessage(String s) {
        return new Message(null,
                s.getBytes(),
                "empty-topic",
                0, 0);
    }
}
