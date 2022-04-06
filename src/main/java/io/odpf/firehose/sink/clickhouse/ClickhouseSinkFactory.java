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
import io.odpf.stencil.Parser;
import io.odpf.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the ClickhouseSink.
 * <p>
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDReporter statsDReporter, StencilClient client)}
 * to obtain the Clickhouse sink implementation.
 */
public class ClickhouseSinkFactory {

    private final Map<String, String> config;
    private final StatsDReporter statsDReporter;
    private final StencilClient stencilClient;
    private QueryTemplate queryTemplate;
    private ClickHouseClient client;
    private ClickHouseNode server;
    private ClickHouseRequest request;

    /**
     * Parameterised Constructor to create Clickhouse sink factory object.
     *
     * @param config         the configuration
     * @param statsDReporter the statsd reporter
     * @param stencilClient  the client
     */

    public ClickhouseSinkFactory(Map<String, String> config, StatsDReporter statsDReporter, StencilClient stencilClient) {
        this.config = config;
        this.statsDReporter = statsDReporter;
        this.stencilClient = stencilClient;
    }

    public void init() {
        ClickhouseSinkConfig clickhouseSinkConfig = ConfigFactory.create(ClickhouseSinkConfig.class, config);
        server = ClickHouseNode.builder()
                .host(clickhouseSinkConfig.getClickhouseHost())
                .port(ClickHouseProtocol.HTTP, Integer.parseInt(clickhouseSinkConfig.getClickhousePort()))
                .database(clickhouseSinkConfig.getClickhouseDatabase()).credentials(ClickHouseCredentials.fromUserAndPassword(
                        clickhouseSinkConfig.getClickhouseUsername(), clickhouseSinkConfig.getClickhousePassword()))
                .build();
        client = ClickHouseClient.builder()
                .nodeSelector(ClickHouseNodeSelector.of(server.getProtocol()))
                .option(ClickHouseClientOption.ASYNC, clickhouseSinkConfig.isClickhouseAsyncModeEnabled())
                .option(ClickHouseClientOption.COMPRESS_ALGORITHM, clickhouseSinkConfig.getClickhouseCompressionAlgorithm()).option(ClickHouseClientOption.COMPRESS, clickhouseSinkConfig.isClickhouseCompressEnabled())
                .option(ClickHouseClientOption.DECOMPRESS_ALGORITHM, clickhouseSinkConfig.getClickhouseCompressionAlgorithm()).option(ClickHouseClientOption.DECOMPRESS, clickhouseSinkConfig.isClickhouseDecompressEnabled())
                .build();
        Instrumentation instrumentation = new Instrumentation(statsDReporter, ClickhouseSinkFactory.class);
        instrumentation.logInfo("Clickhouse connection established");

        request = client.connect(server);
        Parser protoParser = stencilClient.getParser(clickhouseSinkConfig.getInputSchemaProtoClass());
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, clickhouseSinkConfig.getInputSchemaProtoToColumnMapping());
        queryTemplate = new QueryTemplate(clickhouseSinkConfig, protoToFieldMapper);
        queryTemplate.initialize(clickhouseSinkConfig);
    }


    public AbstractSink create() {
        return new ClickhouseSink(new Instrumentation(statsDReporter, ClickhouseSink.class), request, queryTemplate, client);
    }
}
