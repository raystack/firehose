package io.odpf.firehose.sink.clickhouse;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseClientOption;
import io.odpf.firehose.config.ClickhouseSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.proto.ProtoToFieldMapper;
import io.odpf.firehose.sink.AbstractSink;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the ClickhouseSink.
 * <p>
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDReporter statsDReporter, ProtoToFieldMapper protoToFieldMapper)}
 * to obtain the Clickhouse sink implementation.
 */
public class ClickhouseSinkFactory {
    /**
     * x
     * Create Clickhouse sink.
     *
     * @param configuration  the configuration
     * @param statsDReporter the statsd reporter
     * @param protoToFieldMapper  the protoToFieldMapper
     * @return the abstract sink
     */
    public static AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, ProtoToFieldMapper protoToFieldMapper) {
        ClickhouseSinkConfig clickhouseSinkConfig = ConfigFactory.create(ClickhouseSinkConfig.class, configuration);

        Instrumentation instrumentation = new Instrumentation(statsDReporter, ClickhouseSinkFactory.class);

        ClickHouseNode server = ClickHouseNode.builder()
                .host(clickhouseSinkConfig.getClickhouseHost())
                .port(ClickHouseProtocol.HTTP, Integer.parseInt(clickhouseSinkConfig.getClickhousePort()))
                .database(clickhouseSinkConfig.getClickhouseDatabase()).credentials(ClickHouseCredentials.fromUserAndPassword(
                        clickhouseSinkConfig.getClickhouseUsername(), clickhouseSinkConfig.getClickhousePassword()))
                .build();


        ClickHouseClient client = ClickHouseClient.builder()
                .nodeSelector(ClickHouseNodeSelector.of(server.getProtocol()))
                .option(ClickHouseClientOption.ASYNC, clickhouseSinkConfig.isClickhouseAsyncModeEnabled())
                .option(ClickHouseClientOption.COMPRESS_ALGORITHM, clickhouseSinkConfig.getClickhouseCompressionAlgorithm()).option(ClickHouseClientOption.COMPRESS, clickhouseSinkConfig.isClickhouseCompressEnabled())
                .option(ClickHouseClientOption.DECOMPRESS_ALGORITHM, clickhouseSinkConfig.getClickhouseCompressionAlgorithm()).option(ClickHouseClientOption.DECOMPRESS, clickhouseSinkConfig.isClickhouseDecompressEnabled())
                .build();

        instrumentation.logInfo("Clickhouse connection established");

        ClickHouseRequest request = client.connect(server);

        QueryTemplate queryTemplate = new QueryTemplate(clickhouseSinkConfig, protoToFieldMapper);
        queryTemplate.initialize(clickhouseSinkConfig);
        ClickhouseSink clickhouseSink = new ClickhouseSink(new Instrumentation(statsDReporter, ClickhouseSink.class), request, queryTemplate, client);
        return clickhouseSink;
    }
}
