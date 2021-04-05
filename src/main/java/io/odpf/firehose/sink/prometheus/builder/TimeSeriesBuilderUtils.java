package io.odpf.firehose.sink.prometheus.builder;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.odpf.firehose.exception.EglcConfigurationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.odpf.firehose.sink.prometheus.PromSinkConstants.*;

public class TimeSeriesBuilderUtils {

    public static List<PrometheusMetric> getMetricsFromMessage(Message message, Properties metricNameProtoIndexMapping) {
        if (metricNameProtoIndexMapping == null || metricNameProtoIndexMapping.isEmpty()) {
            throw new EglcConfigurationException(FIELD_NAME_MAPPING_ERROR_MESSAGE);
        }
        List<PrometheusMetric> metricList = new ArrayList<>();
        for (Object protoFieldIndex : metricNameProtoIndexMapping.keySet()) {
            PrometheusMetric metric = new PrometheusMetric();
            int fieldIndex = Integer.parseInt((String) protoFieldIndex);
            Object labelKey = metricNameProtoIndexMapping.get(protoFieldIndex);
            Object labelValue = getField(message, fieldIndex);
            if (labelKey instanceof String) {
                metric.setMetricName(labelKey.toString());
                metric.setMetricValue(Double.parseDouble(labelValue.toString()));
                metricList.add(metric);
            } else if (labelKey instanceof Properties) {
                List<PrometheusMetric> metricsFromMessage = getMetricsFromMessage((Message) labelValue, (Properties) labelKey);
                metricList.addAll(metricsFromMessage);
            }
        }
        return metricList;
    }

    public static Map<String, Object> getLabelsFromMessage(Message message, Properties labelNameProtoIndexMapping, int partition) throws InvalidProtocolBufferException {
        Map<String, Object> labelPair = new HashMap<>();
        for (Object protoFieldIndex : labelNameProtoIndexMapping.keySet()) {
            int fieldIndex = Integer.parseInt((String) protoFieldIndex);
            Object label = labelNameProtoIndexMapping.get(protoFieldIndex);
            Object labelValue = getField(message, fieldIndex);
            if (label instanceof String) {
                labelPair.put(label.toString(), getFieldFromString(message, fieldIndex));
            } else if (label instanceof Properties) {
                Map<String, Object> labelsFromMessage = getLabelsFromMessage((Message) labelValue, (Properties) label, partition);
                labelPair.putAll(labelsFromMessage);
            }
        }
        labelPair.put(KAFKA_PARTITION, partition);
        return labelPair;
    }

    private static Object getFieldFromString(Message message, int fieldIndex) throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor fieldDescriptor = getFieldByIndex(message, fieldIndex);
        if (fieldIsOfMessageType(fieldDescriptor, Timestamp.getDescriptor())
                || fieldIsOfMessageType(fieldDescriptor, Duration.getDescriptor())) {
            return getMillisFromTimestamp(getTimestamp(message, fieldIndex));
        } else if (fieldIsOfEnumType(fieldDescriptor)) {
            return getField(message, fieldIndex).toString();
        } else {
            return getField(message, fieldIndex);
        }
    }

    /**
     * Timestamp that will be use as metric timestamp.
     * the timestamp from message or from current timestamp base on prometheus config
     *
     * @param message the protobuf message
     * @return the unix timestamp
     * @throws InvalidProtocolBufferException the exception on invalid protobuf
     */
    public static Long getMetricTimestamp(Message message, boolean isEventTimestampEnabled, int timestampIndex) throws InvalidProtocolBufferException {
        return (isEventTimestampEnabled) ? getMillisFromTimestamp(getTimestamp(message, timestampIndex)) : System.currentTimeMillis();
    }

    private static boolean fieldIsOfMessageType(Descriptors.FieldDescriptor fieldDescriptor, Descriptors.Descriptor typeDescriptor) {
        return fieldDescriptor.getType().name().equals("MESSAGE")
                && fieldDescriptor.getMessageType().getFullName().equals(typeDescriptor.getFullName()
        );
    }

    private static boolean fieldIsOfEnumType(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.getType().name().equals("ENUM");
    }

    private static Timestamp getTimestamp(Message message, Integer fieldIndex) throws InvalidProtocolBufferException {
        DynamicMessage timestamp = (DynamicMessage) getField(message, fieldIndex);
        return Timestamp.parseFrom(timestamp.toByteArray());
    }

    private static Object getField(Message message, int protoIndex) {
        return message.getField(getFieldByIndex(message, protoIndex));
    }

    private static Descriptors.FieldDescriptor getFieldByIndex(Message message, int protoIndex) {
        return message.getDescriptorForType().findFieldByNumber(protoIndex);
    }

    private static Long getMillisFromTimestamp(Timestamp timestamp) {
        Long millisFromSeconds = timestamp.getSeconds() * SECONDS_SCALED_TO_MILLI;
        Long millisFromNanos = timestamp.getNanos() / MILLIS_SCALED_TO_NANOS;
        return millisFromSeconds + millisFromNanos;
    }
}
