package com.gojek.esb.config;

public interface ESSinkConfig extends AppConfig {

    @Key("ES_BATCH_RETRY_COUNT")
    @DefaultValue("3")
    Integer getEsBatchRetryCount();

    @Key("ES_READ_TIMEOUT")
    @DefaultValue("60000")
    Long getEsReadTimeout();

    @Key("ES_CONNECTION_URLS")
    String getEsConnectionUrls();

    @Key("ES_INDEX_NAME")
    String getEsIndexName();

    @Key("ES_RETRY_BACKOFF")
    @DefaultValue("10")
    Long getEsRetryBackoff();

    @Key("ES_BATCH_SIZE")
    @DefaultValue("1000")
    Integer getEsBatchSize();

    @Key("ES_TYPE_NAME")
    String getEsTypeName();

    @Key("ES_ID_FIELD")
    String getEsIdFieldName();

    @Key("ES_UPDATE_ONLY_MODE")
    @DefaultValue("false")
    Boolean isUpdateOnlyMode();

    @Key("ES_INPUT_MESSAGE_TYPE")
    @DefaultValue("JSON")
    String getESMessageType();
}
