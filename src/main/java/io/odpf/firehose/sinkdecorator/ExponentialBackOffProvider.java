package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.metrics.Instrumentation;

import static io.odpf.firehose.metrics.Metrics.RETRY_SLEEP_TIME_MILLISECONDS;
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

    /**
     * Instantiates a new Exponential back off provider.
     *
     * @param initialExpiryTimeInMs the initial expiry time in ms
     * @param backoffRate           the backoff rate
     * @param maximumExpiryTimeInMS the maximum expiry time in ms
     * @param instrumentation       the instrumentation
     * @param backOff               the back off
     */
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
        instrumentation.captureSleepTime(RETRY_SLEEP_TIME_MILLISECONDS, toIntExact(sleepTime));
        backOff.inMilliSeconds(sleepTime);
    }

    private long calculateDelay(int attemptCount) {
        double exponentialBackOffTimeInMs = initialExpiryTimeInMs * Math.pow(backoffRate, attemptCount);
        return (long) Math.min(maximumExpiryTimeInMS, exponentialBackOffTimeInMs);
    }
}
