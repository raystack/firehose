package io.odpf.firehose.tracer;

import io.odpf.firehose.consumer.Message;
import io.opentracing.Span;

import java.util.List;

/**
 * An interface to trace the transactions.
 */
public interface Traceable {

    /**
     * @param messages
     * @return
     */
    List<Span> startTrace(List<Message> messages);

    /**
     * @param spans
     */
    default void finishTrace(List<Span> spans) {
        spans.forEach(span -> span.finish());
    }

}
