package com.gojek.esb.sinkdecorator;

import com.gojek.esb.metrics.Instrumentation;
import static java.lang.Math.toIntExact;

/**
 * ExponentialBackOffProvider is a provider of exponential back-off algorithm.
 * <p>
 * The backoff time is computed as per the following formula.
 * Min{maximumExpiryTimeInMS, initialExpiryTimeInMs * Math.pow(backoffRate,
 * attemptCount)}
 */
public class ExponentialBackOffProvider implements BackOffProvider {

    private final int initialExpiryTimeInMs;
    private final int backoffRate;
    private final int maximumExpiryTimeInMS;
    private Instrumentation instrumentation;
    private final BackOff backOff;

    public ExponentialBackOffProvider(int initialExpiryTimeInMs, int backoffRate, int maximumExpiryTimeInMS,
            Instrumentation instrumentation, BackOff backOff) {
        this.initialExpiryTimeInMs = initialExpiryTimeInMs;
        this.backoffRate = backoffRate;
        this.maximumExpiryTimeInMS = maximumExpiryTimeInMS;
        this.instrumentation = instrumentation;
        this.backOff = backOff;
    }

    @Override
    public void backOff(int attemptCount) {
        long sleepTime = this.calculateDelay(attemptCount);
        instrumentation.logWarn("backing off for {} milliseconds ", sleepTime);
        instrumentation.captureSleepTime("backoff_sleep_time", toIntExact(sleepTime));
        backOff.inMilliSeconds(sleepTime);
    }

    private long calculateDelay(int attemptCount) {
        double exponentialBackOffTimeInMs = initialExpiryTimeInMs * Math.pow(backoffRate, attemptCount);
        return (long) Math.min(maximumExpiryTimeInMS, exponentialBackOffTimeInMs);
    }
}
