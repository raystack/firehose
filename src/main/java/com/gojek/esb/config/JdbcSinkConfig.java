package com.gojek.esb.config;

public interface JdbcSinkConfig extends AppConfig {

    @Key("sink.jdbc.url")
    String getSinkJdbcUrl();

    @Key("sink.jdbc.username")
    String getSinkJdbcUsername();

    @Key("sink.jdbc.password")
    String getSinkJdbcPassword();

    @Key("sink.jdbc.table.name")
    String getSinkJdbcTableName();

    @Key("sink.jdbc.unique.keys")
    @DefaultValue("")
    String getSinkJdbcUniqueKeys();

    @Key("sink.jdbc.connection.pool.max.size")
    Integer getSinkJdbcConnectionPoolMaxSize();

    @Key("sink.jdbc.connection.pool.timeout.ms")
    Integer getSinkJdbcConnectionPoolTimeoutMs();

    @Key("sink.jdbc.connection.pool.idle.timeout.ms")
    Integer getSinkJdbcConnectionPoolIdleTimeoutMs();

    @Key("sink.jdbc.connection.pool.min.idle")
    Integer getSinkJdbcConnectionPoolMinIdle();
}
