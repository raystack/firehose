package io.odpf.firehose.config;

import com.clickhouse.client.ClickHouseCompression;

public interface ClickhouseSinkConfig extends AppConfig{
    @Key("CLICKHOUSE_HOST")
    String getClickhouseHost();

    @Key("CLICKHOUSE_PORT")
    String getClickhousePort();

    @Key("CLICKHOUSE_DATABASE")
    String getClickhouseDatabase();

    @Key("CLICKHOUSE_USERNAME")
    String getClickhouseUsername();

    @Key("CLICKHOUSE_PASSWORD")
    String getClickhousePassword();

    @Key("CLICKHOUSE_TABLE_NAME")
    String getClickhouseTableName();

    @Key("CLICKHOUSE_ASYNC_MODE")
    @DefaultValue("true")
    Boolean getClickhouseAsyncMode();

    @Key("CLICKHOUSE_COMPRESS_ENABLE")
    @DefaultValue("true")
    Boolean isClickhouseCompressEnabled();

    @Key("CLICKHOUSE_DECOMPRESS_ENABLE")
    @DefaultValue("true")
    Boolean isClickhouseDecompressEnabled();

    @Key("CLICKHOUSE_COMPRESSION_ALGORITHM")
    @DefaultValue("lz4")
    String getClickhouseCompressionAlgorithm();

}
