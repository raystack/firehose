package com.gojek.esb.config;

import com.gojek.esb.config.converter.RangeToHashMapConverter;
import com.gojek.esb.config.converter.ProtoIndexToFieldMapConverter;
import org.aeonbits.owner.Config;

import java.util.Map;
import java.util.Properties;

public interface PrometheusSinkConfig extends AppConfig {

    @Key("sink.prom.retry.status.code.ranges")
    @DefaultValue("400-600")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkPromRetryStatusCodeRanges();

    @Key("sink.prom.request.log.status.code.ranges")
    @DefaultValue("400-499")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkPromRequestLogStatusCodeRanges();

    @Key("sink.prom.request.timeout.ms")
    @DefaultValue("10000")
    Integer getSinkPromRequestTimeoutMs();

    @Key("sink.prom.max.connections")
    @DefaultValue("10")
    Integer getSinkPromMaxConnections();

    @Key("sink.prom.service.url")
    String getSinkPromServiceUrl();

    @Key("sink.prom.headers")
    @DefaultValue("")
    String getSinkPromHeaders();

    @Key("sink.prom.oauth2.enable")
    @DefaultValue("false")
    Boolean isSinkPromOAuth2Enable();

    @Key("sink.prom.oauth2.access.token.url")
    @DefaultValue("https://localhost:8888")
    String getSinkPromOAuth2AccessTokenUrl();

    @Key("sink.prom.oauth2.client.name")
    @DefaultValue("client_name")
    String getSinkPromOAuth2ClientName();

    @Key("sink.prom.oauth2.client.secret")
    @DefaultValue("client_secret")
    String getSinkPromOAuth2ClientSecret();

    @Key("sink.prom.oauth2.scope")
    @DefaultValue("scope")
    String getSinkPromOAuth2Scope();

    @Config.Key("sink.prom.metric.name.proto.index.mapping")
    @Config.ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getSinkPromMetricNameProtoIndexMapping();

    @Config.Key("sink.prom.label.name.proto.index.mapping")
    @Config.ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getSinkPromLabelNameProtoIndexMapping();

    @Config.Key("sink.prom.proto.event.timestamp.index")
    Integer getSinkPromProtoEventTimestampIndex();

    @Config.Key("sink.prom.with.event.timestamp")
    @DefaultValue("false")
    boolean isEventTimestampEnabled();
}
