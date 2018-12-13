package com.gojek.esb.config;

import com.gojek.esb.config.converter.FieldNameProtoIndexConverter;
import org.aeonbits.owner.Config;
import java.util.Properties;

public interface InfluxSinkConfig extends DBSinkConfig {
    @Config.Key("FIELD_NAME_PROTO_INDEX_MAPPING")
    @Config.ConverterClass(FieldNameProtoIndexConverter.class)
    Properties getFieldNameProtoIndexMapping();

    @Config.Key("TAG_NAME_PROTO_INDEX_MAPPING")
    @Config.ConverterClass(FieldNameProtoIndexConverter.class)
    Properties getTagNameProtoIndexMapping();

    @Config.Key("MEASUREMENT_NAME")
    String getMeasurementName();

    @Config.Key("PROTO_EVENT_TIMESTAMP_INDEX")
    Integer getEventTimestampIndex();

    @Config.Key("DATABASE_NAME")
    String getDatabaseName();
}

