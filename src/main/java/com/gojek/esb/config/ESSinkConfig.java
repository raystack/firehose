package com.gojek.esb.config;

import com.gojek.esb.config.converter.ESMessageTypeConverter;
import com.gojek.esb.config.enums.ESMessageType;


public interface ESSinkConfig extends AppConfig {

    @Key("ES_WAIT_FOR_ACTIVE_SHARDS_COUNT")
    @DefaultValue("1")
    Integer getEsWaitForActiveShardsCount();

    @Key("ES_REQUEST_TIMEOUT_IN_MS")
    @DefaultValue("60000")
    Long getEsRequestTimeoutInMs();

    @Key("ES_RETRY_STATUS_CODE_BLACKLIST")
    @DefaultValue("404")
    String getEsRetryStatusCodeBlacklist();

    @Key("ES_CONNECTION_URLS")
    String getEsConnectionUrls();

    @Key("ES_INDEX_NAME")
    String getEsIndexName();

    @Key("ES_TYPE_NAME")
    String getEsTypeName();

    @Key("ES_ID_FIELD")
    String getEsIdFieldName();

    @Key("ES_UPDATE_ONLY_MODE")
    @DefaultValue("false")
    Boolean isUpdateOnlyMode();

    @Key("ES_INPUT_MESSAGE_TYPE")
    @ConverterClass(ESMessageTypeConverter.class)
    @DefaultValue("JSON")
    ESMessageType getESMessageType();

    @Key("ES_PRESERVE_PROTO_FIELD_NAMES")
    @DefaultValue("true")
    Boolean shouldPreserveProtoFieldNames();
}
