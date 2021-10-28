package io.odpf.firehose.consumer;

import io.jaegertracing.Configuration;
import io.odpf.firehose.consumer.kafka.ConsumerAndOffsetManager;
import io.odpf.firehose.consumer.kafka.FirehoseKafkaConsumer;
import io.odpf.firehose.consumer.kafka.OffsetManager;
import io.odpf.firehose.utils.KafkaUtils;
import io.odpf.firehose.config.AppConfig;
import io.odpf.firehose.config.DlqConfig;
import io.odpf.firehose.config.FilterConfig;
import io.odpf.firehose.config.ErrorConfig;
import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.config.SinkPoolConfig;
import io.odpf.firehose.config.enums.KafkaConsumerMode;
import io.odpf.firehose.sink.SinkPool;
import io.odpf.firehose.exception.ConfigurationException;
import io.odpf.firehose.filter.Filter;
import io.odpf.firehose.filter.NoOpFilter;
import io.odpf.firehose.filter.jexl.JexlFilter;
import io.odpf.firehose.filter.json.JsonFilter;
import io.odpf.firehose.filter.json.JsonFilterUtil;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sink.bigquery.BigQuerySinkFactory;
import io.odpf.firehose.sink.elasticsearch.EsSinkFactory;
import io.odpf.firehose.sink.grpc.GrpcSinkFactory;
import io.odpf.firehose.sink.http.HttpSinkFactory;
import io.odpf.firehose.sink.influxdb.InfluxSinkFactory;
import io.odpf.firehose.sink.jdbc.JdbcSinkFactory;
import io.odpf.firehose.sink.log.KeyOrMessageParser;
import io.odpf.firehose.sink.log.LogSinkFactory;
import io.odpf.firehose.sink.mongodb.MongoSinkFactory;
import io.odpf.firehose.sink.blob.BlobSinkFactory;
import io.odpf.firehose.sink.prometheus.PromSinkFactory;
import io.odpf.firehose.sink.redis.RedisSinkFactory;
import io.odpf.firehose.sinkdecorator.BackOff;
import io.odpf.firehose.sinkdecorator.BackOffProvider;
import io.odpf.firehose.error.ErrorHandler;
import io.odpf.firehose.sinkdecorator.ExponentialBackOffProvider;
import io.odpf.firehose.sinkdecorator.SinkFinal;
import io.odpf.firehose.sinkdecorator.SinkWithDlq;
import io.odpf.firehose.sinkdecorator.SinkWithFailHandler;
import io.odpf.firehose.sinkdecorator.SinkWithRetry;
import io.odpf.firehose.sink.dlq.DlqWriter;
import io.odpf.firehose.sink.dlq.DlqWriterFactory;
import io.odpf.firehose.tracer.SinkTracer;
import io.odpf.firehose.utils.StencilUtils;
import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.parser.ProtoParser;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import org.aeonbits.owner.ConfigFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Factory for Firehose consumer.
 */
public class FirehoseConsumerFactory {

    private final KafkaConsumerConfig kafkaConsumerConfig;
    private final Map<String, String> config = System.getenv();
    private final StatsDReporter statsDReporter;
    private final StencilClient stencilClient;
    private final Instrumentation instrumentation;
    private final KeyOrMessageParser parser;
    private final OffsetManager offsetManager = new OffsetManager();

    /**
     * Instantiates a new Firehose consumer factory.
     *
     * @param kafkaConsumerConfig the kafka consumer config
     * @param statsDReporter      the stats d reporter
     */
    public FirehoseConsumerFactory(KafkaConsumerConfig kafkaConsumerConfig, StatsDReporter statsDReporter) {
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.statsDReporter = statsDReporter;
        instrumentation = new Instrumentation(this.statsDReporter, FirehoseConsumerFactory.class);

        String additionalConsumerConfig = String.format(""
                        + "\n\tEnable Async Commit: %s"
                        + "\n\tCommit Only Current Partition: %s",
                this.kafkaConsumerConfig.isSourceKafkaAsyncCommitEnable(),
                this.kafkaConsumerConfig.isSourceKafkaCommitOnlyCurrentPartitionsEnable());
        instrumentation.logDebug(additionalConsumerConfig);

        String stencilUrl = this.kafkaConsumerConfig.getSchemaRegistryStencilUrls();
        stencilClient = this.kafkaConsumerConfig.isSchemaRegistryStencilEnable()
                ? StencilClientFactory.getClient(stencilUrl, StencilUtils.getStencilConfig(kafkaConsumerConfig), this.statsDReporter.getClient())
                : StencilClientFactory.getClient();
        parser = new KeyOrMessageParser(new ProtoParser(stencilClient, kafkaConsumerConfig.getInputSchemaProtoClass()), kafkaConsumerConfig);
    }

    private FirehoseFilter buildFilter(FilterConfig filterConfig) {
        instrumentation.logInfo("Filter Engine: {}", filterConfig.getFilterEngine());
        Filter filter;
        switch (filterConfig.getFilterEngine()) {
            case JSON:
                Instrumentation jsonFilterUtilInstrumentation = new Instrumentation(statsDReporter, JsonFilterUtil.class);
                JsonFilterUtil.logConfigs(filterConfig, jsonFilterUtilInstrumentation);
                JsonFilterUtil.validateConfigs(filterConfig, jsonFilterUtilInstrumentation);
                filter = new JsonFilter(stencilClient, filterConfig, new Instrumentation(statsDReporter, JsonFilter.class));
                break;
            case JEXL:
                filter = new JexlFilter(filterConfig, new Instrumentation(statsDReporter, JexlFilter.class));
                break;
            case NO_OP:
                filter = new NoOpFilter(new Instrumentation(statsDReporter, NoOpFilter.class));
                break;
            default:
                throw new IllegalArgumentException("Invalid filter engine type");
        }
        return new FirehoseFilter(filter, new Instrumentation(statsDReporter, FirehoseFilter.class));
    }

    /**
     * Helps to create consumer based on the config.
     *
     * @return FirehoseConsumer firehose consumer
     */
    public FirehoseConsumer buildConsumer() {
        FilterConfig filterConfig = ConfigFactory.create(FilterConfig.class, config);
        FirehoseFilter firehoseFilter = buildFilter(filterConfig);
        Tracer tracer = NoopTracerFactory.create();
        if (kafkaConsumerConfig.isTraceJaegarEnable()) {
            tracer = Configuration.fromEnv("Firehose" + ": " + kafkaConsumerConfig.getSourceKafkaConsumerGroupId()).getTracer();
        }
        FirehoseKafkaConsumer firehoseKafkaConsumer = KafkaUtils.createConsumer(kafkaConsumerConfig, config, statsDReporter, tracer);
        SinkTracer firehoseTracer = new SinkTracer(tracer, kafkaConsumerConfig.getSinkType().name() + " SINK",
                kafkaConsumerConfig.isTraceJaegarEnable());
        if (kafkaConsumerConfig.getSourceKafkaConsumerMode().equals(KafkaConsumerMode.SYNC)) {
            Sink sink = createSink(tracer);
            ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(Collections.singletonList(sink), offsetManager, firehoseKafkaConsumer, kafkaConsumerConfig, new Instrumentation(statsDReporter, ConsumerAndOffsetManager.class));
            return new FirehoseSyncConsumer(
                    sink,
                    firehoseTracer,
                    consumerAndOffsetManager,
                    firehoseFilter,
                    new Instrumentation(statsDReporter, FirehoseSyncConsumer.class));
        } else {
            SinkPoolConfig sinkPoolConfig = ConfigFactory.create(SinkPoolConfig.class, config);
            int nThreads = sinkPoolConfig.getSinkPoolNumThreads();
            List<Sink> sinks = new ArrayList<>(nThreads);
            for (int ii = 0; ii < nThreads; ii++) {
                sinks.add(createSink(tracer));
            }
            ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(sinks, offsetManager, firehoseKafkaConsumer, kafkaConsumerConfig, new Instrumentation(statsDReporter, ConsumerAndOffsetManager.class));
            SinkPool sinkPool = new SinkPool(
                    new LinkedBlockingQueue<>(sinks),
                    Executors.newCachedThreadPool(),
                    sinkPoolConfig.getSinkPoolQueuePollTimeoutMS());
            return new FirehoseAsyncConsumer(
                    sinkPool,
                    firehoseTracer,
                    consumerAndOffsetManager,
                    firehoseFilter,
                    new Instrumentation(statsDReporter, FirehoseAsyncConsumer.class));
        }
    }

    /**
     * return the basic Sink implementation based on the config.
     *
     * @return Sink
     */
    private Sink getSink() {
        instrumentation.logInfo("Sink Type: {}", kafkaConsumerConfig.getSinkType().toString());
        switch (kafkaConsumerConfig.getSinkType()) {
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
                return BigQuerySinkFactory.create(config, statsDReporter);
            case MONGODB:
                return MongoSinkFactory.create(config, statsDReporter, stencilClient);
            default:
                throw new ConfigurationException("Invalid Firehose SINK_TYPE");

        }
    }

    private Sink createSink(Tracer tracer) {
        ErrorHandler errorHandler = new ErrorHandler(ConfigFactory.create(ErrorConfig.class, config));
        Sink baseSink = getSink();
        Sink sinkWithFailHandler = new SinkWithFailHandler(baseSink, errorHandler);
        Sink sinkWithRetry = withRetry(sinkWithFailHandler, errorHandler);
        Sink sinWithDLQ = withDlq(sinkWithRetry, tracer, errorHandler);
        return new SinkFinal(sinWithDLQ, new Instrumentation(statsDReporter, SinkFinal.class));
    }

    public Sink withDlq(Sink sink, Tracer tracer, ErrorHandler errorHandler) {
        DlqConfig dlqConfig = ConfigFactory.create(DlqConfig.class, config);
        if (!dlqConfig.getDlqSinkEnable()) {
            return sink;
        }
        DlqWriter dlqWriter = DlqWriterFactory.create(new HashMap<>(config), statsDReporter, tracer);
        BackOffProvider backOffProvider = getBackOffProvider();
        return new SinkWithDlq(
                sink,
                dlqWriter,
                backOffProvider,
                dlqConfig,
                errorHandler,
                new Instrumentation(statsDReporter, SinkWithDlq.class));
    }

    /**
     * to enable the retry feature for the basic sinks based on the config.
     *
     * @param sink         Sink To wrap with retry decorator
     * @param errorHandler error handler
     * @return Sink with retry decorator
     */
    private Sink withRetry(Sink sink, ErrorHandler errorHandler) {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, config);
        BackOffProvider backOffProvider = getBackOffProvider();
        return new SinkWithRetry(sink, backOffProvider, new Instrumentation(statsDReporter, SinkWithRetry.class), appConfig, parser, errorHandler);
    }

    private BackOffProvider getBackOffProvider() {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, config);
        return new ExponentialBackOffProvider(
                appConfig.getRetryExponentialBackoffInitialMs(),
                appConfig.getRetryExponentialBackoffRate(),
                appConfig.getRetryExponentialBackoffMaxMs(),
                new Instrumentation(statsDReporter, ExponentialBackOffProvider.class),
                new BackOff(new Instrumentation(statsDReporter, BackOff.class)));
    }
}
