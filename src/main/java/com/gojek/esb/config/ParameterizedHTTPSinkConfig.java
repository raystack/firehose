package com.gojek.esb.config;

import com.gojek.esb.config.converter.HttpSinkParameterPlacementTypeConverter;
import com.gojek.esb.config.enums.HttpSinkParameterPlacementType;

public interface ParameterizedHTTPSinkConfig extends HTTPSinkConfig {

    @Key("HTTP_SINK_PARAMETER_PLACEMEMT")
    @DefaultValue("header")
    @ConverterClass(HttpSinkParameterPlacementTypeConverter.class)
    HttpSinkParameterPlacementType getHttpSinkParameterPlacement();
}
