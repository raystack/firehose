package io.odpf.firehose.sink.prometheus.builder;

import io.odpf.firehose.config.PromSinkConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import cortexpb.Cortex;

import java.util.Properties;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import static io.odpf.firehose.sink.prometheus.PromSinkConstants.*;

/**
 * Builder for Cortex TimeSeries.
 */
public class TimeSeriesBuilder {

    private Properties metricNameProtoIndexMapping;
    private Properties labelNameProtoIndexMapping;
    private Integer timestampIndex;
    private Boolean isEventTimestampEnabled;

    /**
     * Instantiates a new cortex time series builder.
     *
     * @param config the prometheus sink config
     */
    public TimeSeriesBuilder(PromSinkConfig config) {
        metricNameProtoIndexMapping = config.getSinkPromMetricNameProtoIndexMapping();
        labelNameProtoIndexMapping = config.getSinkPromLabelNameProtoIndexMapping();
        timestampIndex = config.getSinkPromProtoEventTimestampIndex();
        isEventTimestampEnabled = config.isEventTimestampEnabled();
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

        Map<String, Object> labelPair = TimeSeriesBuilderUtils.getLabelsFromMessage(message, labelNameProtoIndexMapping, partition);
        List<Map<String, Object>> metricList = TimeSeriesBuilderUtils.getMetricsFromMessage(message, metricNameProtoIndexMapping);
        Long metricTimestamp = TimeSeriesBuilderUtils.getMetricTimestamp(message, isEventTimestampEnabled, timestampIndex);
        List<Cortex.TimeSeries> timeSeriesList = new ArrayList<>();
        for (Map<String, Object> metricName : metricList) {
            Cortex.LabelPair metric = buildMetric(metricName.get(METRIC_NAME).toString(), cortexLabelBuilder);
            Cortex.Sample sample = buildSample(metricTimestamp, Double.parseDouble(metricName.get(METRIC_VALUE).toString()), cortexSampleBuilder);
            cortexTimeSeriesBuilder.clear();
            cortexTimeSeriesBuilder.addLabels(metric);
            cortexTimeSeriesBuilder.addSamples(sample);
            for (Map.Entry<String, Object> entry : labelPair.entrySet()) {
                Cortex.LabelPair label = buildLabels(entry.getKey(), entry.getValue(), cortexLabelBuilder);
                cortexTimeSeriesBuilder.addLabels(label);
            }
            timeSeriesList.add(cortexTimeSeriesBuilder.build());
        }
        return timeSeriesList;
    }

    private Cortex.LabelPair buildMetric(String metricName, Cortex.LabelPair.Builder cortexLabelBuilder) {
        cortexLabelBuilder.clear();
        return cortexLabelBuilder
                .setName(PROMETHEUS_LABEL_FOR_METRIC_NAME)
                .setValue(metricName)
                .build();
    }

    private Cortex.LabelPair buildLabels(String labelName, Object labelValue, Cortex.LabelPair.Builder cortexLabelBuilder) {
        cortexLabelBuilder.clear();
        return cortexLabelBuilder
                .setName(labelName)
                .setValue(labelValue.toString())
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
