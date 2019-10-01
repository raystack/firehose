package com.gojek.esb.tracer;

import com.gojek.esb.consumer.EsbMessage;
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
    List<Span> startTrace(List<EsbMessage> messages);

    /**
     * @param spans
     */
    default void finishTrace(List<Span> spans) {
        spans.forEach(span -> span.finish());
    }

}
