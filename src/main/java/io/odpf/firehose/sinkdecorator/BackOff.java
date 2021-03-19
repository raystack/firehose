package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.metrics.Instrumentation;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BackOff {

    private Instrumentation instrumentation;

    public void inMilliSeconds(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            instrumentation.captureNonFatalError(e, "Backoff thread sleep for {} milliseconds interrupted : {} {}",
            milliseconds, e.getClass(), e.getMessage());
        }
    }
}
