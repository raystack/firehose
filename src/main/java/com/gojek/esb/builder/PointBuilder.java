package com.gojek.esb.builder;

import com.gojek.esb.config.InfluxSinkConfig;
import com.gojek.esb.exception.EglcConfigurationException;
import com.google.protobuf.*;
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

    private void addFieldsToPoint(DynamicMessage message) {
        if (fieldNameProtoIndexMapping.isEmpty()) {
            throw new EglcConfigurationException(FIELD_NAME_MAPPING_ERROR_MESSAGE);
        }
        Map fieldNameValueMap = new HashMap<String, Object>();
        for (Object protoFieldIndex : fieldNameProtoIndexMapping.keySet()) {
            Integer fieldIndex = Integer.parseInt((String) protoFieldIndex);
            String influxFieldName = (String) fieldNameProtoIndexMapping.get(protoFieldIndex);

            Object fieldValue = null;
            Descriptors.FieldDescriptor fieldDescriptor = getFieldByIndex(message, fieldIndex);
            if (fieldDescriptor.getType().name().equals("MESSAGE") && fieldDescriptor.getMessageType().getFullName().equals(Timestamp.getDescriptor().getFullName())) {
                try {
                    fieldValue = getMillisFromTimestamp(getTimestamp(message, fieldIndex));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
            } else if (fieldDescriptor.getType().name().equals("MESSAGE") && fieldDescriptor.getMessageType().getFullName().equals(Duration.getDescriptor().getFullName())) {
                try {
                    fieldValue = getMillisFromTimestamp(getTimestamp(message, fieldIndex));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
            } else fieldValue = getField(message, fieldIndex);
            fieldNameValueMap.put(influxFieldName, fieldValue);
        }
        pointBuilder.fields(fieldNameValueMap);
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
