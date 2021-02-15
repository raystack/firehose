package com.gojek.esb.config;

import com.gojek.esb.config.converter.ProtoIndexToFieldMapConverter;
import com.gojek.esb.config.converter.SinkTypeConverter;
import com.gojek.esb.config.enums.SinkType;
import org.aeonbits.owner.Config;

import java.util.Properties;

public interface AppConfig extends Config {

    @Key("statsd.host")
    @DefaultValue("localhost")
    String getStatsDHost();

    @Key("statsd.port")
    @DefaultValue("8125")
    Integer getStatsDPort();

    @Key("statsd.tags")
    @DefaultValue("")
    String getStatsDTags();

    @Key("sink.type")
    @ConverterClass(SinkTypeConverter.class)
    SinkType getSinkType();

    @Key("consumer.threads.num")
    @DefaultValue("1")
    Integer getConsumerThreadsNum();

    @Key("consumer.threads.cleanup.delay")
    @DefaultValue("2000")
    Integer getConsumerThreadsCleanupDelay();

    @Key("stencil.enable")
    @DefaultValue("false")
    Boolean isStencilEnable();

    @Key("stencil.urls")
    String getStencilUrls();

    @Key("proto.schema")
    String getProtoSchema();

    @Key("input.output.mapping")
    @ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getInputOutputMapping();

    @Key("kafka.record.parcer.mode")
    @DefaultValue("message")
    String getKafkaRecordParserMode();

    @Key("trace.enable")
    @DefaultValue("false")
    Boolean isTraceEnable();
}
