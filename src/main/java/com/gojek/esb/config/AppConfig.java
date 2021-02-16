package com.gojek.esb.config;

import com.gojek.esb.config.converter.FilterTypeConverter;
import com.gojek.esb.config.converter.ProtoIndexToFieldMapConverter;
import com.gojek.esb.config.converter.SinkTypeConverter;
import com.gojek.esb.config.enums.FilterType;
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

    @Key("filter.type")
    @ConverterClass(FilterTypeConverter.class)
    @DefaultValue("NONE")
    FilterType getFilterType();

    @Key("filter.expression")
    String getFilterExpression();

    @Key("filter.proto.schema")
    String getFilterProtoSchema();

    @Key("trace.enable")
    @DefaultValue("false")
    Boolean isTraceEnable();

    @Key("retry.exponential.backoff.initial.ms")
    @DefaultValue("10")
    Integer getRetryExponentialBackoffInitialMs();

    @Key("retry.exponential.backoff.rate")
    @DefaultValue("2")
    Integer getRetryExponentialBackoffRate();

    @Key("retry.exponential.backoff.max.ms")
    @DefaultValue("60000")
    Integer getRetryExponentialBackoffMaxMs();

    @Key("retry.queue.enable")
    @DefaultValue("false")
    Boolean isRetryQueueEnable();
}
