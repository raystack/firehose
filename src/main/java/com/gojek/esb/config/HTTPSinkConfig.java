package com.gojek.esb.config;

import com.gojek.esb.config.converter.HttpRequestMethodConverter;
import com.gojek.esb.config.converter.HttpSinkParameterDataFormatConverter;
import com.gojek.esb.config.converter.HttpSinkParameterSourceTypeConverter;
import com.gojek.esb.config.converter.RangeToHashMapConverter;
import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.config.enums.HttpSinkDataFormat;
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

    @Key("HTTP_SINK_REQUEST_METHOD")
    @DefaultValue("put")
    @ConverterClass(HttpRequestMethodConverter.class)
    HttpRequestMethod getHttpSinkRequestMethod();

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

    @Key("HTTP_SINK_DATA_FORMAT")
    @DefaultValue("proto")
    @ConverterClass(HttpSinkParameterDataFormatConverter.class)
    HttpSinkDataFormat getHttpSinkDataFormat();

    @Key("HTTP_SINK_OAUTH2_ENABLED")
    @DefaultValue("false")
    Boolean getHttpSinkOAuth2Enabled();

    @Key("HTTP_SINK_OAUTH2_ACCESS_TOKEN_URL")
    @DefaultValue("https://localhost:8888")
    String getHttpSinkOAuth2AccessTokenURL();

    @Key("HTTP_SINK_OAUTH2_CLIENT_NAME")
    @DefaultValue("client_name")
    String getHttpSinkOAuth2ClientName();

    @Key("HTTP_SINK_OAUTH2_CLIENT_SECRET")
    @DefaultValue("client_secret")
    String getHttpSinkOAuth2ClientSecret();

    @Key("HTTP_SINK_OAUTH2_SCOPE")
    @DefaultValue("scope")
    String getHttpSinkOAuth2Scope();

    @Key("HTTPSINK_JSONBODY_TEMPLATE")
    @DefaultValue("")
    String getHttpSinkJsonBodyTemplate();
}
