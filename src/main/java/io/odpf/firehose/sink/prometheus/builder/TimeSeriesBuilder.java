package io.odpf.firehose.sink.prometheus.builder;

import io.odpf.firehose.config.PromSinkConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import cortexpb.Cortex;

import java.util.Properties;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.odpf.firehose.sink.prometheus.PromSinkConstants.*;

/**
 * Builder for Cortex TimeSeries.
 */
public class TimeSeriesBuilder {

    private final Properties metricNameProtoIndexMapping;
    private final Properties labelNameProtoIndexMapping;
    private Integer timestampIndex;
    private Boolean isEventTimestampEnabled;

    /**
     * Instantiates a new cortex time series builder.
     *
     * @param config the prometheus sink config
     */
    public TimeSeriesBuilder(PromSinkConfig config) {
        this(config.getSinkPromMetricNameProtoIndexMapping(), config.getSinkPromLabelNameProtoIndexMapping());
        isEventTimestampEnabled = config.isEventTimestampEnabled();
        timestampIndex = config.getSinkPromProtoEventTimestampIndex();
    }

    private TimeSeriesBuilder(Properties metricNameProtoIndexMapping, Properties labelNameProtoIndexMapping) {
        this.metricNameProtoIndexMapping = metricNameProtoIndexMapping == null ? new Properties() : metricNameProtoIndexMapping;
        this.labelNameProtoIndexMapping = labelNameProtoIndexMapping == null ? new Properties() : labelNameProtoIndexMapping;
    }

    /**
     * build list of sorted cortex time series object.
     *
     * @param message   the protobuf message
     * @param partition the kafka partition where the message is consumed
     * @return list of sorted cortex time series object
     * @throws InvalidProtocolBufferException the exception on invalid protobuf
     */
    public List<Cortex.TimeSeries> buildTimeSeries(Message message, int partition) throws InvalidProtocolBufferException {
        Cortex.TimeSeries.Builder cortexTimeSeriesBuilder = Cortex.TimeSeries.newBuilder();
        Cortex.LabelPair.Builder cortexLabelBuilder = Cortex.LabelPair.newBuilder();
        Cortex.Sample.Builder cortexSampleBuilder = Cortex.Sample.newBuilder();

        Map<String, String> labels = TimeSeriesBuilderUtils.getLabelsFromMessage(message, labelNameProtoIndexMapping, partition);
        List<PrometheusMetric> metricList = TimeSeriesBuilderUtils.getMetricsFromMessage(message, metricNameProtoIndexMapping);
        Long metricTimestamp = TimeSeriesBuilderUtils.getMetricTimestamp(message, isEventTimestampEnabled, timestampIndex);
        return metricList.stream().map(prometheusMetric -> {
            cortexTimeSeriesBuilder.clear();
            cortexTimeSeriesBuilder.addLabels(buildMetric(prometheusMetric.getMetricName(), cortexLabelBuilder));
            cortexTimeSeriesBuilder.addSamples(buildSample(metricTimestamp, prometheusMetric.getMetricValue(), cortexSampleBuilder));
            labels.forEach((name, value) -> cortexTimeSeriesBuilder.addLabels(buildLabels(name, value, cortexLabelBuilder)));
            return cortexTimeSeriesBuilder.build();
        }).collect(Collectors.toList());
    }

    private Cortex.LabelPair buildMetric(String metricName, Cortex.LabelPair.Builder cortexLabelBuilder) {
        return buildLabels(PROMETHEUS_LABEL_FOR_METRIC_NAME, metricName, cortexLabelBuilder);
    }

    private Cortex.LabelPair buildLabels(String labelName, String labelValue, Cortex.LabelPair.Builder cortexLabelBuilder) {
        cortexLabelBuilder.clear();
        return cortexLabelBuilder
                .setName(labelName)
                .setValue(labelValue)
                .build();
    }

    private Cortex.Sample buildSample(long timestamp, double value, Cortex.Sample.Builder cortexSampleBuilder) {
        cortexSampleBuilder.clear();
        return cortexSampleBuilder
                .setTimestampMs(timestamp)
                .setValue(value)
                .build();
    }
}
