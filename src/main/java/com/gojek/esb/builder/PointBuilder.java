package com.gojek.esb.builder;

import com.gojek.esb.config.InfluxSinkConfig;
import com.gojek.esb.exception.EglcConfigurationException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.influxdb.dto.Point;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PointBuilder {
    public static final String FIELD_NAME_MAPPING_ERROR_MESSAGE = "field index mapping cannot be empty; at least one field value is required";
    private static final long SECONDS_SCALED_TO_MILLI = 1000L;
    private static final long MILLIS_SCALED_TO_NANOS = 1000000L;

    private Point.Builder pointBuilder;
    private Properties tagNameProtoIndexMapping;
    private Properties fieldNameProtoIndexMapping;
    private String measurementName;
    private Integer timeStampIndex;

    public PointBuilder(InfluxSinkConfig config) {
        tagNameProtoIndexMapping = config.getTagNameProtoIndexMapping();
        fieldNameProtoIndexMapping = config.getFieldNameProtoIndexMapping();
        this.timeStampIndex = config.getEventTimestampIndex();
        this.measurementName = config.getMeasurementName();
    }

    public Point buildPoint(DynamicMessage message) throws InvalidProtocolBufferException {
        this.pointBuilder = Point.measurement(measurementName);
        addTagsToPoint(message);
        addFieldsToPoint(message);
        Timestamp timestamp = getTimestamp(message, timeStampIndex);
        pointBuilder.time(getMillisFromTimestamp(timestamp), TimeUnit.MILLISECONDS);
        return pointBuilder.build();
    }

    private Timestamp getTimestamp(DynamicMessage message, Integer fieldIndex) throws InvalidProtocolBufferException {
        DynamicMessage timestamp = (DynamicMessage) getField(message, fieldIndex);
        return Timestamp.parseFrom(timestamp.toByteArray());
    }

    private void addTagsToPoint(DynamicMessage message) {
        for (Object protoFieldIndex : tagNameProtoIndexMapping.keySet()) {
            Object tagValue = getField(message, Integer.parseInt((String) protoFieldIndex));
            String influxTagName = (String) tagNameProtoIndexMapping.get(protoFieldIndex);
            pointBuilder.tag(influxTagName, tagValue.toString());
        }
    }

    private void addFieldsToPoint(DynamicMessage message) throws InvalidProtocolBufferException {
        if (fieldNameProtoIndexMapping.isEmpty()) {
            throw new EglcConfigurationException(FIELD_NAME_MAPPING_ERROR_MESSAGE);
        }
        Map<String, Object> fieldNameValueMap = new HashMap<>();
        for (Object protoFieldIndex : fieldNameProtoIndexMapping.keySet()) {
            int fieldIndex = Integer.parseInt((String) protoFieldIndex);
            String influxFieldName = (String) fieldNameProtoIndexMapping.get(protoFieldIndex);

            Descriptors.FieldDescriptor fieldDescriptor = getFieldByIndex(message, fieldIndex);
            if (fieldIsOfMessageType(fieldDescriptor, Timestamp.getDescriptor())
                    || fieldIsOfMessageType(fieldDescriptor, Duration.getDescriptor())) {
                fieldNameValueMap.put(influxFieldName, getMillisFromTimestamp(getTimestamp(message, fieldIndex)));
            } else {
                fieldNameValueMap.put(influxFieldName, getField(message, fieldIndex));
            }
        }
        pointBuilder.fields(fieldNameValueMap);
    }

    private boolean fieldIsOfMessageType(Descriptors.FieldDescriptor fieldDescriptor, Descriptors.Descriptor typeDescriptor) {
        return fieldDescriptor.getType().name().equals("MESSAGE")
                && fieldDescriptor.getMessageType().getFullName().equals(typeDescriptor.getFullName()
        );
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
