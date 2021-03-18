package com.gojek.esb.sinkdecorator;

import com.gojek.esb.metrics.Instrumentation;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static java.lang.Math.toIntExact;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ExponentialBackOffProviderTest {

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private BackOff backOff;

    private final int initialExpiryTimeInMS = 10;

    private final int maximumBackoffTimeInMS = 1000 * 60;

    private final int backOffRate = 2;

    private ExponentialBackOffProvider exponentialBackOffProvider;

    @Before
    public void setup() {
        exponentialBackOffProvider = new ExponentialBackOffProvider(initialExpiryTimeInMS, backOffRate,
                maximumBackoffTimeInMS, instrumentation, backOff);
    }

    @Test
    public void shouldBeWithinMaxBackoffTime() {
        exponentialBackOffProvider.backOff(100000000);
        verify(backOff).inMilliSeconds(maximumBackoffTimeInMS);

        verify(instrumentation, times(1)).logWarn("backing off for {} milliseconds ", (long) maximumBackoffTimeInMS);
        verify(instrumentation, times(1)).captureSleepTime("firehose_retry_backoff_sleep_milliseconds", toIntExact(maximumBackoffTimeInMS));
    }

    @Test
    public void shouldBackoffExponentially() {
        exponentialBackOffProvider.backOff(1);
        long sleepTime1 = 20;
        verify(backOff).inMilliSeconds(sleepTime1);

        verify(instrumentation, times(1)).logWarn("backing off for {} milliseconds ", sleepTime1);
        verify(instrumentation, times(1)).captureSleepTime("firehose_retry_backoff_sleep_milliseconds", toIntExact(sleepTime1));

        exponentialBackOffProvider.backOff(4);
        long sleepTime2 = 160;
        verify(backOff).inMilliSeconds(sleepTime2);

        verify(instrumentation, times(1)).logWarn("backing off for {} milliseconds ", sleepTime2);
        verify(instrumentation, times(1)).captureSleepTime("firehose_retry_backoff_sleep_milliseconds", toIntExact(sleepTime2));
    }

    @Test
    public void shouldSleepForBackOffTimeOnFirstRetry() {
        exponentialBackOffProvider.backOff(0);

        verify(backOff).inMilliSeconds(initialExpiryTimeInMS);
    }

    @Test
    public void shouldRecordStatsForBackOffTime() {
        exponentialBackOffProvider.backOff(0);

        verify(instrumentation, times(1)).logWarn("backing off for {} milliseconds ", (long) initialExpiryTimeInMS);
        verify(instrumentation).captureSleepTime("firehose_retry_backoff_sleep_milliseconds", initialExpiryTimeInMS);
    }
}
