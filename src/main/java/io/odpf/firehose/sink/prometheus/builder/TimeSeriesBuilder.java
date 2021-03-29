package io.odpf.firehose.sink.prometheus.builder;

import io.odpf.firehose.config.PromSinkConfig;
import io.odpf.firehose.exception.EglcConfigurationException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Duration;

import cortexpb.Cortex;

import java.util.Properties;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

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
     * @param config    the prometheus sink config
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
     * @param message                           the protobuf message
     * @param partition                         the kafka partition where the message is consumed
     * @return                                  list of sorted cortex time series object
     * @throws InvalidProtocolBufferException   the exception on invalid protobuf
     */
    public List<Cortex.TimeSeries> buildTimeSeries(Message message, int partition) throws InvalidProtocolBufferException {
        Cortex.TimeSeries.Builder cortexTimeSeriesBuilder = Cortex.TimeSeries.newBuilder();
        Cortex.LabelPair.Builder cortexLabelBuilder = Cortex.LabelPair.newBuilder();
        Cortex.Sample.Builder cortexSampleBuilder = Cortex.Sample.newBuilder();

        Map<String, Object> labelPair = getLabelMessage(message, labelNameProtoIndexMapping, partition);
        List<Map<String, Object>> metricList = getMetricMessage(message, metricNameProtoIndexMapping);
        List<Cortex.TimeSeries> timeSeriesList = new ArrayList<>();
        Long metricTimestamp = getMetricTimestamp(message);
        for (Map<String, Object> metricName : metricList) {
            cortexTimeSeriesBuilder.clear();
            buildMetric(metricName.get(METRIC_NAME).toString(), cortexTimeSeriesBuilder, cortexLabelBuilder);
            for (Map.Entry<String, Object> entry : labelPair.entrySet()) {
                buildLabels(entry.getKey(), entry.getValue(), cortexTimeSeriesBuilder, cortexLabelBuilder);
            }
            buildSample(metricTimestamp, Double.parseDouble(metricName.get(METRIC_VALUE).toString()), cortexTimeSeriesBuilder, cortexSampleBuilder);
            timeSeriesList.add(cortexTimeSeriesBuilder.build());
        }
        return timeSeriesList;
    }

    private void buildMetric(String metricName, Cortex.TimeSeries.Builder cortexTimeSeriesBuilder, Cortex.LabelPair.Builder cortexLabelBuilder) {
        cortexLabelBuilder.clear();
        Cortex.LabelPair metric = cortexLabelBuilder
                .setName(PROMETHEUS_LABEL_FOR_METRIC_NAME)
                .setValue(metricName)
                .build();
        cortexTimeSeriesBuilder.addLabels(metric);
    }

    private void buildLabels(String labelName, Object labelValue, Cortex.TimeSeries.Builder cortexTimeSeriesBuilder, Cortex.LabelPair.Builder cortexLabelBuilder) {
        cortexLabelBuilder.clear();
        Cortex.LabelPair label = cortexLabelBuilder
                .setName(labelName)
                .setValue(labelValue.toString())
                .build();
        cortexTimeSeriesBuilder.addLabels(label);
    }

    private void buildSample(long timestamp, double value, Cortex.TimeSeries.Builder cortexTimeSeriesBuilder, Cortex.Sample.Builder cortexSampleBuilder) {
        cortexSampleBuilder.clear();
        Cortex.Sample sample = cortexSampleBuilder
                .setTimestampMs(timestamp)
                .setValue(value)
                .build();
        cortexTimeSeriesBuilder.addSamples(sample);
    }

    /**
     * Timestamp that will be use as metric timestamp.
     * the timestamp from message or from current timestamp base on prometheus config
     *
     * @param message                           the protobuf message
     * @return                                  the unix timestamp
     * @throws InvalidProtocolBufferException   the exception on invalid protobuf
     */
    private Long getMetricTimestamp(Message message) throws InvalidProtocolBufferException {
        return (isEventTimestampEnabled) ? getMillisFromTimestamp(getTimestamp(message, timestampIndex)) : System.currentTimeMillis();
    }

    private List<Map<String, Object>> getMetricMessage(Message message, Properties protoIndexMapping) {
        if (protoIndexMapping == null || protoIndexMapping.isEmpty()) {
            throw new EglcConfigurationException(FIELD_NAME_MAPPING_ERROR_MESSAGE);
        }
        List<Map<String, Object>> metricList = new ArrayList<>();
        for (Object protoFieldIndex : protoIndexMapping.keySet()) {
            Map<String, Object> labelPair = new HashMap<>();
            int fieldIndex = Integer.parseInt((String) protoFieldIndex);
            Object labelValue = getField(message, fieldIndex);
            Object label = protoIndexMapping.get(protoFieldIndex);
            if (label instanceof String) {
                labelPair.put(METRIC_NAME, label);
                labelPair.put(METRIC_VALUE, labelValue);
            } else if (label instanceof Properties) {
                return getMetricMessage((Message) labelValue, (Properties) label);
            }
            metricList.add(labelPair);
        }
        return metricList;
    }

    private Map<String, Object> getLabelMessage(Message message, Properties protoIndexMapping, int partition) throws InvalidProtocolBufferException {
        Map<String, Object> labelPair = new HashMap<>();
        if (protoIndexMapping == null) {
            labelPair.put(KAFKA_PARTITION, partition);
        } else {
            for (Object protoFieldIndex : protoIndexMapping.keySet()) {
                int fieldIndex = Integer.parseInt((String) protoFieldIndex);
                Object labelValue = getField(message, fieldIndex);
                Object label = protoIndexMapping.get(protoFieldIndex);
                if (label instanceof String) {
                    Descriptors.FieldDescriptor fieldDescriptor = getFieldByIndex(message, fieldIndex);
                    if (fieldIsOfMessageType(fieldDescriptor, Timestamp.getDescriptor())
                            || fieldIsOfMessageType(fieldDescriptor, Duration.getDescriptor())) {
                        labelPair.put((String) label, getMillisFromTimestamp(getTimestamp(message, fieldIndex)));
                    } else if (fieldIsOfEnumType(fieldDescriptor)) {
                        labelPair.put((String) label, getField(message, fieldIndex).toString());
                    } else {
                        labelPair.put((String) label, getField(message, fieldIndex));
                    }
                } else if (label instanceof Properties) {
                    return getLabelMessage((Message) labelValue, (Properties) label, partition);
                }
            }
            labelPair.put(KAFKA_PARTITION, partition);
        }
        return labelPair;
    }

    private boolean fieldIsOfMessageType(Descriptors.FieldDescriptor fieldDescriptor, Descriptors.Descriptor typeDescriptor) {
        return fieldDescriptor.getType().name().equals("MESSAGE")
                && fieldDescriptor.getMessageType().getFullName().equals(typeDescriptor.getFullName()
        );
    }

    private boolean fieldIsOfEnumType(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.getType().name().equals("ENUM");
    }

    private Timestamp getTimestamp(Message message, Integer fieldIndex) throws InvalidProtocolBufferException {
        DynamicMessage timestamp = (DynamicMessage) getField(message, fieldIndex);
        return Timestamp.parseFrom(timestamp.toByteArray());
    }

    private Object getField(Message message, int protoIndex) {
        return message.getField(getFieldByIndex(message, protoIndex));
    }

    private Descriptors.FieldDescriptor getFieldByIndex(Message message, int protoIndex) {
        return message.getDescriptorForType().findFieldByNumber(protoIndex);
    }

    private Long getMillisFromTimestamp(Timestamp timestamp) {
        Long millisFromSeconds = timestamp.getSeconds() * SECONDS_SCALED_TO_MILLI;
        Long millisFromNanos = timestamp.getNanos() / MILLIS_SCALED_TO_NANOS;
        return millisFromSeconds + millisFromNanos;
    }
}
