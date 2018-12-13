package com.gojek.esb.metrics;

import com.gojek.esb.util.Clock;
import com.timgroup.statsd.StatsDClient;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;


public class StatsDReporter {

    private Optional<StatsDClient> client;
    private String globalTags;

    private Clock clock;

    public StatsDReporter(Optional<StatsDClient> client, Clock clock, String... globalTags) {
        this.client = client;
        this.globalTags = String.join(",", globalTags).replaceAll(":", "=");
        this.clock = clock;
    }

    public Clock getClock() {
        return clock;
    }

    public Optional<StatsDClient> getClient() {
        return client;
    }

    public void captureCount(String metric, Integer delta, String... tags) {
        client.ifPresent(c -> c.count(withTags(metric, tags), delta));
    }

    public void captureCount(String metric, Integer delta) {
        client.ifPresent(c -> c.count(withGlobalTags(metric), delta));
    }

    public void captureDurationSince(String metric, Instant startTime, String... tags) {
        client.ifPresent(c -> c.recordExecutionTime(withTags(metric, tags), Duration.between(startTime, clock.now()).toMillis()));
    }

    public void gauge(String metric, Integer value) {
        client.ifPresent(c -> c.gauge(withGlobalTags(metric), value));
    }

    public void increment(String metric, String... tags) {
        captureCount(metric, 1, tags);
    }

    public void increment(String metric) {
        captureCount(metric, 1);
    }

    private String withGlobalTags(String metric) {
        return metric + "," + this.globalTags;
    }

    private String withTags(String metric, String... tags) {
        return metric + "," + this.globalTags + "," + String.join(",", tags);
    }

}
