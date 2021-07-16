package io.odpf.firehose.config;

import io.odpf.firehose.config.converter.SetErrorTypeConverter;
import io.odpf.firehose.error.ErrorType;
import org.aeonbits.owner.Config;

import java.util.Set;

public interface ErrorConfig extends Config {

    @ConverterClass(SetErrorTypeConverter.class)
    @Key("ERROR_TYPES_FOR_DLQ")
    @Separator(",")
    @DefaultValue("DESERIALIZATION_ERROR")
    Set<ErrorType> getErrorTypesForDLQ();

    @ConverterClass(SetErrorTypeConverter.class)
    @Key("ERROR_TYPES_FOR_RETRY")
    @Separator(",")
    @DefaultValue("")
    Set<ErrorType> getErrorTypesForRetry();

    @ConverterClass(SetErrorTypeConverter.class)
    @Key("ERROR_TYPES_FOR_FAILING")
    @Separator(",")
    @DefaultValue("")
    Set<ErrorType> getErrorTypesForFailing();

}
