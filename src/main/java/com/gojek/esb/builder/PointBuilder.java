package com.gojek.esb.builder;

import com.gojek.esb.config.InfluxSinkConfig;
import com.gojek.esb.exception.EglcConfigurationException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.influxdb.dto.Point;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PointBuilder {
    public static final String FIELD_NAME_MAPPING_ERROR_MESSAGE = "field index mapping cannot be empty; at least one field value is required";
    private static final long SECONDS_SCALED_TO_MILLI = 1000L;

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

    public Point buildPoint(DynamicMessage message) {
        this.pointBuilder = Point.measurement(measurementName);
        addTagsToPoint(message);
        addFieldsToPoint(message);
        Timestamp timestamp = getTimestamp(message);
        pointBuilder.time(timestamp.getSeconds() * SECONDS_SCALED_TO_MILLI, TimeUnit.MILLISECONDS);
        return pointBuilder.build();
    }

    private Timestamp getTimestamp(DynamicMessage message) {
        DynamicMessage timestamp = (DynamicMessage) getField(message, timeStampIndex);
        List<Descriptors.FieldDescriptor> timestampFields = timestamp.getDescriptorForType().getFields();
        Timestamp.Builder timestampBuilder = Timestamp.newBuilder();
        timestampFields
                .forEach(fieldDescriptor -> timestampBuilder.setField(fieldDescriptor, timestamp.getField(fieldDescriptor)));
        return timestampBuilder.build();
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
            String influxFieldName = (String) fieldNameProtoIndexMapping.get(protoFieldIndex);
            System.out.println(influxFieldName);
            Object fieldValue = getField(message, Integer.parseInt((String) protoFieldIndex));
            System.out.println(protoFieldIndex);
            System.out.println(fieldValue);
            fieldNameValueMap.put(influxFieldName, fieldValue);
        }
        pointBuilder.fields(fieldNameValueMap);
    }

    private Object getField(Message message, int protoIndex) {
        return message.getField(message.getDescriptorForType().findFieldByNumber(protoIndex));
    }


}
