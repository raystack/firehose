package io.odpf.firehose.sink.file;

import java.time.Duration;
import java.time.Instant;

public class TimeBasedRotatingPolicy implements RotatingFilePolicy {

    private final Duration duration;

    private Instant endTime;
    private Instant registeredTime;

    public TimeBasedRotatingPolicy(Duration duration) {
        this.duration = duration;
    }

    public void start() {
        Instant now = Instant.now();
        endTime = now.plus(duration);
        registeredTime = now;
    }

    @Override
    public boolean needRotate() {
        return endTime.isBefore(registeredTime);
    }

    public void setRegisteredTime(Instant registeredTime) {
        this.registeredTime = registeredTime;
    }

    public void advanceTime(Duration duration) {
        registeredTime.plus(duration);
    }
}
