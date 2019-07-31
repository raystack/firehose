package com.gojek.esb.config;

import com.gojek.esb.config.converter.ProtoIndexToFieldMapConverter;
import com.gojek.esb.config.converter.SinkConverter;
import com.gojek.esb.config.enums.SinkType;
import org.aeonbits.owner.Config;

import java.util.Properties;

public interface AppConfig extends Config {

    @Key("STATSD_HOST")
    @DefaultValue("localhost")
    String getStatsDHost();

    @Key("STATSD_PORT")
    @DefaultValue("8125")
    Integer getStatsDPort();

    @Key("STATSD_TAGS")
    @DefaultValue("")
    String getStatsDTags();

    @Key("SINK")
    @ConverterClass(SinkConverter.class)
    SinkType getSinkType();

    @Key("NUMBER_OF_CONSUMERS_THREADS")
    @DefaultValue("1")
    Integer noOfConsumerThreads();

    @Key("DELAY_TO_CLEAN_UP_CONSUMER_THREADS")
    @DefaultValue("2000")
    Integer threadCleanupDelay();

    @Key("ENABLE_STENCIL_CLIENT")
    @DefaultValue("false")
    Boolean enableStencilClient();

    @Key("STENCIL_URL")
    String stencilUrl();

    @Key("PROTO_SCHEMA")
    String getProtoSchema();

    @Key("PROTO_TO_COLUMN_MAPPING")
    @ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getProtoToFieldMapping();

    @Key("KAFKA_RECORD_PARSER_MODE")
    @DefaultValue("message")
    String getKafkaRecordParserMode();
}
