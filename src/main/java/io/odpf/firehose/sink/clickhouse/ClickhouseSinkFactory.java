package io.odpf.firehose.sink.clickhouse;

import com.clickhouse.client.*;
import com.clickhouse.client.config.ClickHouseClientOption;
import io.odpf.firehose.config.ClickhouseSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.proto.ProtoToFieldMapper;
import io.odpf.firehose.sink.Sink;
import io.odpf.stencil.Parser;
import io.odpf.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

public class ClickhouseSinkFactory {
    public static Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        ClickhouseSinkConfig clickhouseSinkConfig = ConfigFactory.create(ClickhouseSinkConfig.class, configuration);

        Instrumentation instrumentation = new Instrumentation(statsDReporter, ClickhouseSinkFactory.class);

        ClickHouseNode server = ClickHouseNode.builder()
                .host(clickhouseSinkConfig.getClickhouseHost())
                .port(ClickHouseProtocol.HTTP, Integer.parseInt(clickhouseSinkConfig.getClickhousePort()))
                .database(clickhouseSinkConfig.getClickhouseDatabase()).credentials(ClickHouseCredentials.fromUserAndPassword(
                        clickhouseSinkConfig.getClickhouseUsername(), clickhouseSinkConfig.getClickhousePassword()))
                .build();

        ClickHouseCompression clickHouseCompression = null;

        if(clickhouseSinkConfig.getClickhouseCompressionAlgorithm().equals("lz4")){
            clickHouseCompression = ClickHouseCompression.LZ4;
        } else if(clickhouseSinkConfig.getClickhouseCompressionAlgorithm().equals("gzip")){
            clickHouseCompression = ClickHouseCompression.GZIP;
        }

        ClickHouseClient client = ClickHouseClient.builder()
                .nodeSelector(ClickHouseNodeSelector.of(server.getProtocol()))
                .option(ClickHouseClientOption.ASYNC, clickhouseSinkConfig.getClickhouseAsyncMode())
                .option(ClickHouseClientOption.COMPRESS_ALGORITHM,clickHouseCompression).option(ClickHouseClientOption.COMPRESS,clickhouseSinkConfig.isClickhouseCompressEnabled())
                .option(ClickHouseClientOption.DECOMPRESS_ALGORITHM,clickHouseCompression).option(ClickHouseClientOption.DECOMPRESS,clickhouseSinkConfig.isClickhouseDecompressEnabled())
                .build();

        instrumentation.logInfo("Clickhouse connection established");

        ClickHouseRequest request = client.connect(server);
        QueryTemplate queryTemplate = createQueryTemplate(clickhouseSinkConfig, stencilClient);
        ClickhouseSink clickhouseSink = new ClickhouseSink(new Instrumentation(statsDReporter, ClickhouseSink.class),request,queryTemplate);
        return clickhouseSink;
    }

    private static QueryTemplate createQueryTemplate(ClickhouseSinkConfig clickhouseSinkConfig, StencilClient stencilClient) {
        Parser protoParser = stencilClient.getParser(clickhouseSinkConfig.getInputSchemaProtoClass());
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, clickhouseSinkConfig.getInputSchemaProtoToColumnMapping());
        return new QueryTemplate(clickhouseSinkConfig, protoToFieldMapper);
    }
}
