package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.consumer.ErrorInfo;
import io.odpf.firehose.consumer.ErrorType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.sink.Sink;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SinkWithFailHandlerTest {

    @Mock
    private Sink sink;

    private final ErrorMatcher errorMatcher = new ErrorMatcher(false, new HashSet<>());
    private SinkWithFailHandler sinkWithFailHandler;

    @Before
    public void setUp() throws Exception {
        sinkWithFailHandler = new SinkWithFailHandler(sink, errorMatcher);
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenMessageContainsConfiguredError() throws IOException {
        Set<ErrorType> failErrors = new HashSet<>();
        failErrors.add(ErrorType.DESERIALIZATION_ERROR);

        List<Message> messages = new LinkedList<>();
        messages.add(new Message("".getBytes(), "".getBytes(), "basic", 1, 1,
                null, 0, 0,
                new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR)));

        when(sink.pushMessage(anyList())).thenReturn(messages);

        sinkWithFailHandler = new SinkWithFailHandler(sink, new ErrorMatcher(false, failErrors));
        sinkWithFailHandler.pushMessage(messages);
    }

    @Test
    public void shouldNotThrowIOExceptionWhenNoMessageContainsErrorType() throws IOException {
        Set<ErrorType> failErrors = new HashSet<>();
        failErrors.add(ErrorType.DESERIALIZATION_ERROR);

        List<Message> messages = new LinkedList<>();
        messages.add(new Message("".getBytes(), "".getBytes(), "basic", 1, 1,
                null, 0, 0,
                new ErrorInfo(null, null)));

        when(sink.pushMessage(anyList())).thenReturn(messages);

        sinkWithFailHandler = new SinkWithFailHandler(sink, new ErrorMatcher(false, failErrors));
        sinkWithFailHandler.pushMessage(messages);
    }

    @Test
    public void shouldNotThrowIOExceptionWhenNoMessageContainsErrorInfo() throws IOException {
        Set<ErrorType> failErrors = new HashSet<>();
        failErrors.add(ErrorType.DESERIALIZATION_ERROR);

        List<Message> messages = new LinkedList<>();
        messages.add(new Message("".getBytes(), "".getBytes(), "basic", 1, 1,
                null, 0, 0));

        when(sink.pushMessage(anyList())).thenReturn(messages);

        sinkWithFailHandler = new SinkWithFailHandler(sink, new ErrorMatcher(false, failErrors));
        sinkWithFailHandler.pushMessage(messages);
    }

    @Test
    public void shouldNotThrowIOExceptionWhenConfiguredErrorIsEmpty() throws IOException {
        List<Message> messages = new LinkedList<>();
        messages.add(new Message("".getBytes(), "".getBytes(), "basic", 1, 1,
                null, 0, 0,
                new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR)));

        when(sink.pushMessage(anyList())).thenReturn(messages);

        sinkWithFailHandler.pushMessage(messages);
    }
}
