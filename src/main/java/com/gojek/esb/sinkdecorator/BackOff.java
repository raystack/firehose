package com.gojek.esb.sinkdecorator;

import com.gojek.esb.metrics.StatsDReporter;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BackOff {

    private Instrumentation instrumentation;

    public static BackOff withInstrumentationFactory(StatsDReporter statsDReporter) {
        Instrumentation instrumentation = new Instrumentation(statsDReporter);
        return new BackOff(instrumentation);
    }

    public void inMilliSeconds(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            instrumentation.captureBackOffThreadInteruptedError(e, milliseconds);
        }
    }
}
