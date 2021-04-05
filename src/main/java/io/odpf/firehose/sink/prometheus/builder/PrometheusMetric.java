package io.odpf.firehose.sink.prometheus.builder;

import lombok.Data;

@Data
public class PrometheusMetric {
    private String metricName;
    private double metricValue;
}
