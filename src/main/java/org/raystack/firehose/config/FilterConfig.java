package org.raystack.firehose.config;

import org.raystack.firehose.config.converter.FilterDataSourceTypeConverter;
import org.raystack.firehose.config.converter.FilterEngineTypeConverter;
import org.raystack.firehose.config.converter.FilterMessageFormatTypeConverter;
import org.raystack.firehose.config.enums.FilterDataSourceType;
import org.raystack.firehose.config.enums.FilterEngineType;
import org.raystack.firehose.config.enums.FilterMessageFormatType;
import org.aeonbits.owner.Config;

public interface FilterConfig extends Config {

    @Key("FILTER_ENGINE")
    @ConverterClass(FilterEngineTypeConverter.class)
    @DefaultValue("NO_OP")
    FilterEngineType getFilterEngine();

    @Key("FILTER_SCHEMA_PROTO_CLASS")
    String getFilterSchemaProtoClass();

    @Key("FILTER_ESB_MESSAGE_FORMAT")
    @ConverterClass(FilterMessageFormatTypeConverter.class)
    FilterMessageFormatType getFilterESBMessageFormat();

    @Key("FILTER_DATA_SOURCE")
    @ConverterClass(FilterDataSourceTypeConverter.class)
    FilterDataSourceType getFilterDataSource();

    @Key("FILTER_JEXL_EXPRESSION")
    String getFilterJexlExpression();

    @Key("FILTER_JSON_SCHEMA")
    String getFilterJsonSchema();

}
