package io.odpf.firehose.config;

import io.odpf.firehose.config.converter.FilterDataSourceTypeConverter;
import io.odpf.firehose.config.converter.FilterEngineTypeConverter;
import io.odpf.firehose.config.converter.FilterMessageFormatTypeConverter;
import io.odpf.firehose.config.enums.FilterDataSourceType;
import io.odpf.firehose.config.enums.FilterEngineType;
import io.odpf.firehose.config.enums.FilterMessageFormatType;
import org.aeonbits.owner.Config;

public interface FilterConfig extends Config {

    @Key("FILTER_ENGINE")
    @ConverterClass(FilterEngineTypeConverter.class)
    @DefaultValue("JEXL")
    FilterEngineType getFilterEngine();

    @Key("FILTER_ESB_MESSAGE_FORMAT")
    @ConverterClass(FilterMessageFormatTypeConverter.class)
    FilterMessageFormatType getFilterMessageFormat();

    @Key("FILTER_JEXL_DATA_SOURCE")
    @ConverterClass(FilterDataSourceTypeConverter.class)
    @DefaultValue("NONE")
    FilterDataSourceType getFilterJexlDataSource();

    @Key("FILTER_JEXL_EXPRESSION")
    String getFilterJexlExpression();

    @Key("FILTER_JEXL_SCHEMA_PROTO_CLASS")
    String getFilterJexlSchemaProtoClass();

    @Key("FILTER_JSON_DATA_SOURCE")
    @ConverterClass(FilterDataSourceTypeConverter.class)
    @DefaultValue("NONE")
    FilterDataSourceType getFilterJsonDataSource();

    @Key("FILTER_JSON_SCHEMA")
    String getFilterJsonSchema();

    @Key("FILTER_JSON_SCHEMA_PROTO_CLASS")
    String getFilterJsonSchemaProtoClass();

}
