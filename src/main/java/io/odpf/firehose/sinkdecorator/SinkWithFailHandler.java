package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.sink.Sink;

import java.io.IOException;
import java.util.List;

/**
 * Sink that will throw exception when error match configuration.
 * This is intended to be used to trigger consumer failure based on configured error types
 */
public class SinkWithFailHandler extends SinkDecorator {
    private final ErrorMatcher failMatcher;
    /**
     * Instantiates a new Sink decorator.
     *
     * @param sink        wrapped sink object
     * @param failMatcher
     */
    public SinkWithFailHandler(Sink sink, ErrorMatcher failMatcher) {
        super(sink);
        this.failMatcher = failMatcher;
    }

    @Override
    public List<Message> pushMessage(List<Message> messages) throws IOException, DeserializerException {
        List<Message> handledMessages = super.pushMessage(messages);

        for (Message message : handledMessages) {
            if (failMatcher.isMatch(message)) {
                throw new IOException(message.getErrorInfo().getException());
            }
        }

        return handledMessages;
    }
}
