package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.type.Message;
import io.odpf.firehose.error.ErrorHandler;
import io.odpf.firehose.error.ErrorScope;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.exception.SinkException;
import io.odpf.firehose.sink.Sink;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Sink that will throw exception when error match configuration.
 * This is intended to be used to trigger consumer failure based on configured error types
 */
public class SinkWithFailHandler extends SinkDecorator {
    private final ErrorHandler errorHandler;

    /**
     * Instantiates a new Sink decorator.
     *
     * @param sink         wrapped sink object
     * @param errorHandler to process errors
     */
    public SinkWithFailHandler(Sink sink, ErrorHandler errorHandler) {
        super(sink);
        this.errorHandler = errorHandler;
    }

    @Override
    public List<Message> pushMessage(List<Message> inputMessages) throws IOException, DeserializerException {
        List<Message> messages = super.pushMessage(inputMessages);
        Optional<Message> messageOptional = messages.stream().filter(x -> errorHandler.filter(x, ErrorScope.FAIL)).findFirst();
        if (messageOptional.isPresent()) {
            throw new SinkException("Failing Firehose for error " + messageOptional.get().getErrorInfo().getErrorType(),
                    messageOptional.get().getErrorInfo().getException());
        }
        return messages;
    }
}
