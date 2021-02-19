package com.gojek.esb.config;

import com.gojek.esb.config.converter.ProtoIndexToFieldMapConverter;
import org.aeonbits.owner.Config;

import java.util.Properties;

public interface InfluxSinkConfig extends AppConfig {
    @Config.Key("sink.influx.field.name.proto.index.mapping")
    @Config.ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getSinkInfluxFieldNameProtoIndexMapping();

    @Config.Key("sink.influx.tag.name.proto.index.mapping")
    @Config.ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getSinkInfluxTagNameProtoIndexMapping();

    @Config.Key("sink.influx.measurement.name")
    String getSinkInfluxMeasurementName();

    @Config.Key("sink.influx.proto.event.timestamp.index")
    Integer getSinkInfluxProtoEventTimestampIndex();

    @Config.Key("sink.influx.db.name")
    String getSinkInfluxDbName();

    @Config.Key("sink.influx.retention.policy")
    @DefaultValue("autogen")
    String getSinkInfluxRetentionPolicy();

    @Config.Key("sink.influx.url")
    String getSinkInfluxUrl();

    @Config.Key("sink.influx.username")
    String getSinkInfluxUsername();

    @Config.Key("sink.influx.password")
    String getSinkInfluxPassword();
}

