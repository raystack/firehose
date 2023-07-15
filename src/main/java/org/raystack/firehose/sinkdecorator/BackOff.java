package org.raystack.firehose.sinkdecorator;

import org.raystack.firehose.metrics.FirehoseInstrumentation;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BackOff {

    private FirehoseInstrumentation firehoseInstrumentation;

    public void inMilliSeconds(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            firehoseInstrumentation.captureNonFatalError("firehose_error_event", e, "Backoff thread sleep for {} milliseconds interrupted : {} {}",
                    milliseconds, e.getClass(), e.getMessage());
        }
    }
}
