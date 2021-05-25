package io.odpf.firehose.sink.file;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class TimeBasedRotatingPolicyTest {
    @Test
    public void shouldNeedRotateWhenRegisteredTimeBeyondDefinedWindowOfDuration() {
        TimeBasedRotatingPolicy timeBasedRotatingPolicy = new TimeBasedRotatingPolicy(Duration.ofMillis(2000));
        timeBasedRotatingPolicy.start();
        timeBasedRotatingPolicy.setRegisteredTime(Instant.now().plus(1, ChronoUnit.HOURS));

        boolean needRotate = timeBasedRotatingPolicy.needRotate();
        assertTrue(needRotate);
    }
    @Test
    public void shouldNotNeedRotateWhenRegisteredTimeIsWithinDefinedWindowOfDuration() {
        TimeBasedRotatingPolicy timeBasedRotatingPolicy = new TimeBasedRotatingPolicy(Duration.ofHours(1));
        timeBasedRotatingPolicy.start();
        boolean needRotate = timeBasedRotatingPolicy.needRotate();
        assertFalse(needRotate);
    }
}