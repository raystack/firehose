package com.gojek.esb.config;

import com.gojek.esb.config.converter.HttpSinkParameterSourceTypeConverter;
import com.gojek.esb.config.converter.RangeToHashMapConverter;
import com.gojek.esb.config.enums.HttpSinkParameterSourceType;

import java.util.Map;

public interface HTTPSinkConfig extends AppConfig {

    @Key("HTTPSINK_RETRY_STATUS_CODE_RANGES")
    @DefaultValue("400-600")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> retryStatusCodeRanges();

    @Key("HTTPSINK_REQUEST_TIMEOUT_IN_MS")
    @DefaultValue("10000")
    Integer getRequestTimeoutInMs();

    @Key("HTTPSINK_MAX_HTTP_CONNECTIONS")
    @DefaultValue("10")
    Integer getMaxHttpConnections();

    @Key("SERVICE_URL")
    String getServiceURL();

    @Key("HTTP_HEADERS")
    @DefaultValue("")
    String getHTTPHeaders();

    @Key("HTTP_SINK_PARAMETER_SOURCE")
    @DefaultValue("disabled")
    @ConverterClass(HttpSinkParameterSourceTypeConverter.class)
    HttpSinkParameterSourceType getHttpSinkParameterSource();

}
