package com.gojek.esb.config;

import com.gojek.esb.config.converter.EsSinkMessageTypeConverter;
import com.gojek.esb.config.enums.EsSinkMessageType;


public interface EsSinkConfig extends AppConfig {

    @Key("sink.es.shards.active.wait.count")
    @DefaultValue("1")
    Integer getSinkEsShardsActiveWaitCount();

    @Key("sink.es.request.timeout.ms")
    @DefaultValue("60000")
    Long getSinkEsRequestTimeoutMs();

    @Key("sink.es.retry.status.code.blacklist")
    @DefaultValue("404")
    String getSinkEsRetryStatusCodeBlacklist();

    @Key("sink.es.connection.urls")
    String getSinkEsConnectionUrls();

    @Key("sink.es.index.name")
    String getSinkEsIndexName();

    @Key("sink.es.type.name")
    String getSinkEsTypeName();

    @Key("sink.es.id.field")
    String getSinkEsIdField();

    @Key("sink.es.mode.update.only.enable")
    @DefaultValue("false")
    Boolean isSinkEsModeUpdateOnlyEnable();

    @Key("sink.es.input.message.type")
    @ConverterClass(EsSinkMessageTypeConverter.class)
    @DefaultValue("JSON")
    EsSinkMessageType getSinkEsInputMessageType();

    @Key("sink.es.preserve.proto.field.names.enable")
    @DefaultValue("true")
    Boolean isSinkEsPreserveProtoFieldNamesEnable();

    @Key("sink.es.routing.key.name")
    String getSinkEsRoutingKeyName();
}
