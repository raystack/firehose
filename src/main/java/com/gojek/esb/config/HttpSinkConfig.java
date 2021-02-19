package com.gojek.esb.config;

import com.gojek.esb.config.converter.HttpSinkRequestMethodConverter;
import com.gojek.esb.config.converter.HttpSinkParameterDataFormatConverter;
import com.gojek.esb.config.converter.HttpSinkParameterPlacementTypeConverter;
import com.gojek.esb.config.converter.HttpSinkParameterSourceTypeConverter;
import com.gojek.esb.config.converter.RangeToHashMapConverter;
import com.gojek.esb.config.enums.HttpSinkRequestMethodType;
import com.gojek.esb.config.enums.HttpSinkDataFormatType;
import com.gojek.esb.config.enums.HttpSinkParameterPlacementType;
import com.gojek.esb.config.enums.HttpSinkParameterSourceType;

import java.util.Map;

public interface HttpSinkConfig extends AppConfig {

    @Key("sink.http.retry.status.code.ranges")
    @DefaultValue("400-600")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkHttpRetryStatusCodeRanges();

    @Key("sink.http.request.log.status.code.ranges")
    @DefaultValue("400-499")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkHttpRequestLogStatusCodeRanges();

    @Key("sink.http.request.timeout.ms")
    @DefaultValue("10000")
    Integer getSinkHttpRequestTimeoutMs();

    @Key("sink.http.request.method")
    @DefaultValue("put")
    @ConverterClass(HttpSinkRequestMethodConverter.class)
    HttpSinkRequestMethodType getSinkHttpRequestMethod();

    @Key("sink.http.max.connections")
    @DefaultValue("10")
    Integer getSinkHttpMaxConnections();

    @Key("sink.http.service.url")
    String getSinkHttpServiceUrl();

    @Key("sink.http.headers")
    @DefaultValue("")
    String getSinkHttpHeaders();

    @Key("sink.http.parameter.source")
    @DefaultValue("disabled")
    @ConverterClass(HttpSinkParameterSourceTypeConverter.class)
    HttpSinkParameterSourceType getSinkHttpParameterSource();

    @Key("sink.http.data.format")
    @DefaultValue("proto")
    @ConverterClass(HttpSinkParameterDataFormatConverter.class)
    HttpSinkDataFormatType getSinkHttpDataFormat();

    @Key("sink.http.oauth2.enable")
    @DefaultValue("false")
    Boolean isSinkHttpOAuth2Enable();

    @Key("sink.http.oauth2.access.token.url")
    @DefaultValue("https://localhost:8888")
    String getSinkHttpOAuth2AccessTokenUrl();

    @Key("sink.http.oauth2.client.name")
    @DefaultValue("client_name")
    String getSinkHttpOAuth2ClientName();

    @Key("sink.http.oauth2.client.secret")
    @DefaultValue("client_secret")
    String getSinkHttpOAuth2ClientSecret();

    @Key("sink.http.oauth2.scope")
    @DefaultValue("scope")
    String getSinkHttpOAuth2Scope();

    @Key("sink.http.json.body.template")
    @DefaultValue("")
    String getSinkHttpJsonBodyTemplate();

    @Key("sink.http.parameter.placement")
    @DefaultValue("header")
    @ConverterClass(HttpSinkParameterPlacementTypeConverter.class)
    HttpSinkParameterPlacementType getSinkHttpParameterPlacement();

    @Key("sink.http.parameter.proto.schema")
    String getSinkHttpParameterProtoSchema();

}
