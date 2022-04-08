package io.odpf.firehose.sink;

import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.consumer.kafka.OffsetManager;
import io.odpf.firehose.exception.ConfigurationException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.bigquery.BigQuerySinkFactory;
import io.odpf.firehose.sink.blob.BlobSinkFactory;
import io.odpf.firehose.sink.clickhouse.ClickhouseSinkFactory;
import io.odpf.firehose.sink.elasticsearch.EsSinkFactory;
import io.odpf.firehose.sink.grpc.GrpcSinkFactory;
import io.odpf.firehose.sink.http.HttpSinkFactory;
import io.odpf.firehose.sink.influxdb.InfluxSinkFactory;
import io.odpf.firehose.sink.jdbc.JdbcSinkFactory;
import io.odpf.firehose.sink.log.LogSinkFactory;
import io.odpf.firehose.sink.mongodb.MongoSinkFactory;
import io.odpf.firehose.sink.prometheus.PromSinkFactory;
import io.odpf.firehose.sink.redis.RedisSinkFactory;
import io.odpf.stencil.client.StencilClient;

import java.util.Map;

public class SinkFactory {
    private final KafkaConsumerConfig kafkaConsumerConfig;
    private final StatsDReporter statsDReporter;
    private final Instrumentation instrumentation;
    private final StencilClient stencilClient;
    private final OffsetManager offsetManager;
    private BigQuerySinkFactory bigQuerySinkFactory;
    private ClickhouseSinkFactory clickhouseSinkFactory;
    private final Map<String, String> config = System.getenv();

    public SinkFactory(KafkaConsumerConfig kafkaConsumerConfig,
                       StatsDReporter statsDReporter,
                       StencilClient stencilClient,
                       OffsetManager offsetManager) {
        instrumentation = new Instrumentation(statsDReporter, SinkFactory.class);
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.statsDReporter = statsDReporter;
        this.stencilClient = stencilClient;
        this.offsetManager = offsetManager;
    }

    /**
     * Initialization method for all the sinks.
     */
    public void init() {
        switch (this.kafkaConsumerConfig.getSinkType()) {
            case CLICKHOUSE:
                clickhouseSinkFactory = new ClickhouseSinkFactory(config, statsDReporter, stencilClient);
                clickhouseSinkFactory.init();
                return;
            case JDBC:
            case HTTP:
            case INFLUXDB:
            case LOG:
            case ELASTICSEARCH:
            case REDIS:
            case GRPC:
            case PROMETHEUS:
            case BLOB:
            case MONGODB:
                return;
            case BIGQUERY:
                bigQuerySinkFactory = new BigQuerySinkFactory(config, statsDReporter);
                bigQuerySinkFactory.init();
                return;
            default:
                throw new ConfigurationException("Invalid Firehose SINK_TYPE");
        }
    }

    public Sink getSink() {
        instrumentation.logInfo("Sink Type: {}", kafkaConsumerConfig.getSinkType().toString());
        switch (kafkaConsumerConfig.getSinkType()) {
            case CLICKHOUSE:
                return clickhouseSinkFactory.create();
            case JDBC:
                return JdbcSinkFactory.create(config, statsDReporter, stencilClient);
            case HTTP:
                return HttpSinkFactory.create(config, statsDReporter, stencilClient);
            case INFLUXDB:
                return InfluxSinkFactory.create(config, statsDReporter, stencilClient);
            case LOG:
                return LogSinkFactory.create(config, statsDReporter, stencilClient);
            case ELASTICSEARCH:
                return EsSinkFactory.create(config, statsDReporter, stencilClient);
            case REDIS:
                return RedisSinkFactory.create(config, statsDReporter, stencilClient);
            case GRPC:
                return GrpcSinkFactory.create(config, statsDReporter, stencilClient);
            case PROMETHEUS:
                return PromSinkFactory.create(config, statsDReporter, stencilClient);
            case BLOB:
                return BlobSinkFactory.create(config, offsetManager, statsDReporter, stencilClient);
            case BIGQUERY:
                return bigQuerySinkFactory.create();
            case MONGODB:
                return MongoSinkFactory.create(config, statsDReporter, stencilClient);
            default:
                throw new ConfigurationException("Invalid Firehose SINK_TYPE");
        }
    }
}
