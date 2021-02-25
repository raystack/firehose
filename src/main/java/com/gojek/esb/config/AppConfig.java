package com.gojek.esb.config;

import com.gojek.esb.config.converter.FilterTypeConverter;
import com.gojek.esb.config.converter.ProtoIndexToFieldMapConverter;
import com.gojek.esb.config.converter.SinkTypeConverter;
import com.gojek.esb.config.enums.FilterType;
import com.gojek.esb.config.enums.SinkType;
import org.aeonbits.owner.Config;

import java.util.Properties;

public interface AppConfig extends Config {

    @Key("metric.statsd.host")
    @DefaultValue("localhost")
    String getMetricStatsDHost();

    @Key("metric.statsd.port")
    @DefaultValue("8125")
    Integer getMetricStatsDPort();

    @Key("metric.statsd.tags")
    @DefaultValue("")
    String getMetricStatsDTags();

    @Key("sink.type")
    @ConverterClass(SinkTypeConverter.class)
    SinkType getSinkType();

    @Key("application.thread.count")
    @DefaultValue("1")
    Integer getApplicationThreadCount();

    @Key("application.thread.cleanup.delay")
    @DefaultValue("2000")
    Integer getApplicationThreadCleanupDelay();

    @Key("schema.registry.stencil.enable")
    @DefaultValue("false")
    Boolean isSchemaRegistryStencilEnable();

    @Key("schema.registry.stencil.urls")
    String getSchemaRegistryStencilUrls();

    @Key("input.schema.proto.class")
    String getInputSchemaProtoClass();

    @Key("input.schema.proto.to.column.mapping")
    @ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getInputSchemaProtoToColumnMapping();

    @Key("kafka.record.parser.mode")
    @DefaultValue("message")
    String getKafkaRecordParserMode();

    @Key("filter.jexl.data.source")
    @ConverterClass(FilterTypeConverter.class)
    @DefaultValue("NONE")
    FilterType getFilterJexlDataSource();

    @Key("filter.jexl.expression")
    String getFilterJexlExpression();

    @Key("filter.jexl.proto.schema")
    String getFilterJexlProtoSchema();

    @Key("trace.jaegar.enable")
    @DefaultValue("false")
    Boolean isTraceJaegarEnable();

    @Key("retry.exponential.backoff.initial.ms")
    @DefaultValue("10")
    Integer getRetryExponentialBackoffInitialMs();

    @Key("retry.exponential.backoff.rate")
    @DefaultValue("2")
    Integer getRetryExponentialBackoffRate();

    @Key("retry.exponential.backoff.max.ms")
    @DefaultValue("60000")
    Integer getRetryExponentialBackoffMaxMs();

    @Key("dlq.enable")
    @DefaultValue("false")
    Boolean isDlqEnable();
}
