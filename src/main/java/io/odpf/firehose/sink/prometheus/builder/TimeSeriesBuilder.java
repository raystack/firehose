package io.odpf.firehose.sink.prometheus.builder;

import io.odpf.firehose.config.PrometheusSinkConfig;
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

    private Cortex.TimeSeries.Builder timeSeriesBuilder = Cortex.TimeSeries.newBuilder();
    private Cortex.LabelPair.Builder labelBuilder = Cortex.LabelPair.newBuilder();
    private Cortex.Sample.Builder sampleBuilder = Cortex.Sample.newBuilder();

    private Properties metricNameProtoIndexMapping;
    private Properties labelNameProtoIndexMapping;
    private Integer timestampIndex;
    private Boolean isEventTimestampEnabled;

    /**
     * Instantiates a new cortex time series builder.
     *
     * @param config    the prometheus sink config
     */
    public TimeSeriesBuilder(PrometheusSinkConfig config) {
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
        Map<String, Object> labelPair = getLabelMessage(message, labelNameProtoIndexMapping, partition);
        List<Map<String, Object>> metricList = getMetricMessage(message, metricNameProtoIndexMapping);
        List<Cortex.TimeSeries> timeSeriesList = new ArrayList<>();
        Long metricTimestamp = getMetricTimestamp(message);
        for (Map<String, Object> metricName : metricList) {
            buildMetric((String) metricName.get(METRIC_NAME));
            for (Map.Entry<String, Object> entry : labelPair.entrySet()) {
                buildLabels(entry.getKey(), entry.getValue());
            }
            buildSample(metricTimestamp, Double.parseDouble(metricName.get(METRIC_VALUE).toString()));
            timeSeriesList.add(timeSeriesBuilder.build());
            timeSeriesBuilder.clear();
        }
        return timeSeriesList;
    }

    private void buildMetric(String metricName) {
        Cortex.LabelPair metric = labelBuilder.setName(DEFAULT_LABEL_NAME).setValue(metricName).build();
        timeSeriesBuilder.addLabels(metric);
        labelBuilder.clear();
    }

    private void buildLabels(String labelName, Object labelValue) {
        Cortex.LabelPair label = labelBuilder.setName(labelName).setValue(labelValue.toString()).build();
        timeSeriesBuilder.addLabels(label);
        labelBuilder.clear();
    }

    private void buildSample(long timestamp, double value) {
        Cortex.Sample sample = sampleBuilder.setTimestampMs(timestamp).setValue(value).build();
        timeSeriesBuilder.addSamples(sample);
        sampleBuilder.clear();
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
            Object tagValue = getField(message, fieldIndex);
            Object tag = protoIndexMapping.get(protoFieldIndex);
            if (tag instanceof String) {
                labelPair.put(METRIC_NAME, tag);
                labelPair.put(METRIC_VALUE, tagValue);
            } else if (tag instanceof Properties) {
                return getMetricMessage((Message) tagValue, (Properties) tag);
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
                Object tagValue = getField(message, fieldIndex);
                Object tag = protoIndexMapping.get(protoFieldIndex);
                if (tag instanceof String) {
                    Descriptors.FieldDescriptor fieldDescriptor = getFieldByIndex(message, fieldIndex);
                    if (fieldIsOfMessageType(fieldDescriptor, Timestamp.getDescriptor())
                            || fieldIsOfMessageType(fieldDescriptor, Duration.getDescriptor())) {
                        labelPair.put((String) tag, getMillisFromTimestamp(getTimestamp(message, fieldIndex)));
                    } else if (fieldIsOfEnumType(fieldDescriptor)) {
                        labelPair.put((String) tag, getField(message, fieldIndex).toString());
                    } else {
                        labelPair.put((String) tag, getField(message, fieldIndex));
                    }
                } else if (tag instanceof Properties) {
                    return getLabelMessage((Message) tagValue, (Properties) tag, partition);
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
