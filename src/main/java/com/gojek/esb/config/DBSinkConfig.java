package com.gojek.esb.config;

public interface DBSinkConfig extends AppConfig {

    @Key("DB_URL")
    String getDbUrl();

    @Key("DB_USERNAME")
    String getUser();

    @Key("DB_PASSWORD")
    String getPassword();

    @Key("TABLE_NAME")
    String getTableName();

    @Key("UNIQUE_KEYS")
    @DefaultValue("")
    String getUniqueKeys();

    @Key("MAXIMUM_CONNECTION_POOL_SIZE")
    Integer getMaximumConnectionPoolSize();

    @Key("DB_CONNECTION_TIMEOUT_MS")
    Integer getConnectionTimeout();

    @Key("DB_IDLE_TIMEOUT_MS")
    Integer getIdleTimeout();

    @Key("DB_MINIMUM_IDLE")
    Integer getMinimumIdle();

    @Key("DB_SINK_AUDIT_ENABLED")
    @DefaultValue("False")
    Boolean getAuditEnabled();
}
