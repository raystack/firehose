package io.odpf.firehose.config;

import com.clickhouse.client.ClickHouseCompression;
import io.odpf.firehose.sink.clickhouse.ClickhouseCompressionConverter;

public interface ClickhouseSinkConfig extends AppConfig {
    @Key("SINK_CLICKHOUSE_HOST")
    String getClickhouseHost();

    @Key("SINK_CLICKHOUSE_PORT")
    String getClickhousePort();

    @Key("SINK_CLICKHOUSE_DATABASE")
    @DefaultValue("default")
    String getClickhouseDatabase();

    @Key("SINK_CLICKHOUSE_USERNAME")
    String getClickhouseUsername();

    @Key("SINK_CLICKHOUSE_PASSWORD")
    String getClickhousePassword();

    @Key("SINK_CLICKHOUSE_TABLE_NAME")
    String getClickhouseTableName();

    @Key("SINK_CLICKHOUSE_ASYNC_MODE_ENABLE")
    @DefaultValue("true")
    Boolean isClickhouseAsyncModeEnabled();

    @Key("SINK_CLICKHOUSE_COMPRESS_ENABLE")
    @DefaultValue("true")
    Boolean isClickhouseCompressEnabled();

    @Key("SINK_CLICKHOUSE_DECOMPRESS_ENABLE")
    @DefaultValue("true")
    Boolean isClickhouseDecompressEnabled();

    @Key("SINK_CLICKHOUSE_COMPRESSION_ALGORITHM")
    @DefaultValue("lz4")
    @ConverterClass(ClickhouseCompressionConverter.class)
    ClickHouseCompression getClickhouseCompressionAlgorithm();

}
