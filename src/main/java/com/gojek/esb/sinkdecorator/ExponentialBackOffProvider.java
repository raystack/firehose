package com.gojek.esb.sinkdecorator;

import com.gojek.esb.metrics.StatsDReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final StatsDReporter statsDReporter;
    private final BackOff backOff;
    private static final Logger LOGGER = LoggerFactory.getLogger(ExponentialBackOffProvider.class);

    public ExponentialBackOffProvider(int initialExpiryTimeInMs, int backoffRate, int maximumExpiryTimeInMS,
            StatsDReporter statsDReporter, BackOff backOff) {
        this.initialExpiryTimeInMs = initialExpiryTimeInMs;
        this.backoffRate = backoffRate;
        this.maximumExpiryTimeInMS = maximumExpiryTimeInMS;
        this.statsDReporter = statsDReporter;
        this.backOff = backOff;
    }

    @Override
    public void backOff(int attemptCount) {
        long sleepTime = this.calculateDelay(attemptCount);
        LOGGER.info("backing off for {} milliseconds ", sleepTime);
        backOff.inMilliSeconds(sleepTime);
        statsDReporter.gauge("backoff_time", toIntExact(sleepTime));
    }

    private long calculateDelay(int attemptCount) {
        double exponentialBackOffTimeInMs = initialExpiryTimeInMs * Math.pow(backoffRate, attemptCount);
        return (long) Math.min(maximumExpiryTimeInMS, exponentialBackOffTimeInMs);
    }
}
