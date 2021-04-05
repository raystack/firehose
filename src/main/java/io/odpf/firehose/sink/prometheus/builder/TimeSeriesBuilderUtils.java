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

    public static List<Map<String, Object>> getMetricsFromMessage(Message message, Properties metricNameProtoIndexMapping) {
        if (metricNameProtoIndexMapping == null || metricNameProtoIndexMapping.isEmpty()) {
            throw new EglcConfigurationException(FIELD_NAME_MAPPING_ERROR_MESSAGE);
        }
        List<Map<String, Object>> metricList = new ArrayList<>();
        for (Object protoFieldIndex : metricNameProtoIndexMapping.keySet()) {
            Map<String, Object> labelPair = new HashMap<>();
            int fieldIndex = Integer.parseInt((String) protoFieldIndex);
            Object labelKey = metricNameProtoIndexMapping.get(protoFieldIndex);
            Object labelValue = getField(message, fieldIndex);
            if (labelKey instanceof String) {
                labelPair.put(METRIC_NAME, labelKey);
                labelPair.put(METRIC_VALUE, labelValue);
            } else if (labelKey instanceof Properties) {
                return getMetricsFromMessage((Message) labelValue, (Properties) labelKey);
            }
            metricList.add(labelPair);
        }
        return metricList;
    }

    public static Map<String, Object> getLabelsFromMessage(Message message, Properties labelNameProtoIndexMapping, int partition) throws InvalidProtocolBufferException {
        Map<String, Object> labelPair = new HashMap<>();
        if (labelNameProtoIndexMapping != null) {
            for (Object protoFieldIndex : labelNameProtoIndexMapping.keySet()) {
                int fieldIndex = Integer.parseInt((String) protoFieldIndex);
                Object labelValue = getField(message, fieldIndex);
                Object label = labelNameProtoIndexMapping.get(protoFieldIndex);
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
                    return getLabelsFromMessage((Message) labelValue, (Properties) label, partition);
                }
            }
        }
        labelPair.put(KAFKA_PARTITION, partition);
        return labelPair;
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
