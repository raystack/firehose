package org.raystack.firehose.consumer;

import org.raystack.firehose.consumer.kafka.ConsumerAndOffsetManager;
import org.raystack.firehose.consumer.kafka.FirehoseKafkaConsumer;
import org.raystack.firehose.consumer.kafka.OffsetManager;
import io.jaegertracing.Configuration;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.firehose.metrics.FirehoseInstrumentation;
import org.raystack.firehose.sink.SinkFactory;
import org.raystack.firehose.utils.KafkaUtils;
import org.raystack.firehose.config.AppConfig;
import org.raystack.firehose.config.DlqConfig;
import org.raystack.firehose.config.FilterConfig;
import org.raystack.firehose.config.ErrorConfig;
import org.raystack.firehose.config.KafkaConsumerConfig;
import org.raystack.firehose.config.SinkPoolConfig;
import org.raystack.firehose.config.enums.KafkaConsumerMode;
import org.raystack.firehose.sink.SinkPool;
import org.raystack.firehose.filter.Filter;
import org.raystack.firehose.filter.NoOpFilter;
import org.raystack.firehose.filter.jexl.JexlFilter;
import org.raystack.firehose.filter.json.JsonFilter;
import org.raystack.firehose.filter.json.JsonFilterUtil;
import org.raystack.firehose.sink.Sink;
import org.raystack.firehose.sink.common.KeyOrMessageParser;
import org.raystack.firehose.sinkdecorator.BackOff;
import org.raystack.firehose.sinkdecorator.BackOffProvider;
import org.raystack.firehose.error.ErrorHandler;
import org.raystack.firehose.sinkdecorator.ExponentialBackOffProvider;
import org.raystack.firehose.sinkdecorator.SinkFinal;
import org.raystack.firehose.sinkdecorator.SinkWithDlq;
import org.raystack.firehose.sinkdecorator.SinkWithFailHandler;
import org.raystack.firehose.sinkdecorator.SinkWithRetry;
import org.raystack.firehose.sink.dlq.DlqWriter;
import org.raystack.firehose.sink.dlq.DlqWriterFactory;
import org.raystack.firehose.tracer.SinkTracer;
import org.raystack.firehose.utils.StencilUtils;
import org.raystack.stencil.StencilClientFactory;
import org.raystack.stencil.client.StencilClient;
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
    private final FirehoseInstrumentation firehoseInstrumentation;
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
        firehoseInstrumentation = new FirehoseInstrumentation(this.statsDReporter, FirehoseConsumerFactory.class);

        String additionalConsumerConfig = String.format(""
                        + "\n\tEnable Async Commit: %s"
                        + "\n\tCommit Only Current Partition: %s",
                this.kafkaConsumerConfig.isSourceKafkaAsyncCommitEnable(),
                this.kafkaConsumerConfig.isSourceKafkaCommitOnlyCurrentPartitionsEnable());
        firehoseInstrumentation.logDebug(additionalConsumerConfig);

        String stencilUrl = this.kafkaConsumerConfig.getSchemaRegistryStencilUrls();
        stencilClient = this.kafkaConsumerConfig.isSchemaRegistryStencilEnable()
                ? StencilClientFactory.getClient(stencilUrl, StencilUtils.getStencilConfig(kafkaConsumerConfig, statsDReporter.getClient()))
                : StencilClientFactory.getClient();
        parser = new KeyOrMessageParser(stencilClient.getParser(kafkaConsumerConfig.getInputSchemaProtoClass()), kafkaConsumerConfig);
    }

    private FirehoseFilter buildFilter(FilterConfig filterConfig) {
        firehoseInstrumentation.logInfo("Filter Engine: {}", filterConfig.getFilterEngine());
        Filter filter;
        switch (filterConfig.getFilterEngine()) {
            case JSON:
                FirehoseInstrumentation jsonFilterUtilFirehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, JsonFilterUtil.class);
                JsonFilterUtil.logConfigs(filterConfig, jsonFilterUtilFirehoseInstrumentation);
                JsonFilterUtil.validateConfigs(filterConfig, jsonFilterUtilFirehoseInstrumentation);
                filter = new JsonFilter(stencilClient, filterConfig, new FirehoseInstrumentation(statsDReporter, JsonFilter.class));
                break;
            case JEXL:
                filter = new JexlFilter(filterConfig, new FirehoseInstrumentation(statsDReporter, JexlFilter.class));
                break;
            case NO_OP:
                filter = new NoOpFilter(new FirehoseInstrumentation(statsDReporter, NoOpFilter.class));
                break;
            default:
                throw new IllegalArgumentException("Invalid filter engine type");
        }
        return new FirehoseFilter(filter, new FirehoseInstrumentation(statsDReporter, FirehoseFilter.class));
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
        SinkFactory sinkFactory = new SinkFactory(kafkaConsumerConfig, statsDReporter, stencilClient, offsetManager);
        sinkFactory.init();
        if (kafkaConsumerConfig.getSourceKafkaConsumerMode().equals(KafkaConsumerMode.SYNC)) {
            Sink sink = createSink(tracer, sinkFactory);
            ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(Collections.singletonList(sink), offsetManager, firehoseKafkaConsumer, kafkaConsumerConfig, new FirehoseInstrumentation(statsDReporter, ConsumerAndOffsetManager.class));
            return new FirehoseSyncConsumer(
                    sink,
                    firehoseTracer,
                    consumerAndOffsetManager,
                    firehoseFilter,
                    new FirehoseInstrumentation(statsDReporter, FirehoseSyncConsumer.class));
        } else {
            SinkPoolConfig sinkPoolConfig = ConfigFactory.create(SinkPoolConfig.class, config);
            int nThreads = sinkPoolConfig.getSinkPoolNumThreads();
            List<Sink> sinks = new ArrayList<>(nThreads);
            for (int ii = 0; ii < nThreads; ii++) {
                sinks.add(createSink(tracer, sinkFactory));
            }
            ConsumerAndOffsetManager consumerAndOffsetManager = new ConsumerAndOffsetManager(sinks, offsetManager, firehoseKafkaConsumer, kafkaConsumerConfig, new FirehoseInstrumentation(statsDReporter, ConsumerAndOffsetManager.class));
            SinkPool sinkPool = new SinkPool(
                    new LinkedBlockingQueue<>(sinks),
                    sinks,
                    Executors.newCachedThreadPool(),
                    sinkPoolConfig.getSinkPoolQueuePollTimeoutMS());
            return new FirehoseAsyncConsumer(
                    sinkPool,
                    firehoseTracer,
                    consumerAndOffsetManager,
                    firehoseFilter,
                    new FirehoseInstrumentation(statsDReporter, FirehoseAsyncConsumer.class));
        }
    }

    private Sink createSink(Tracer tracer, SinkFactory sinkFactory) {
        ErrorHandler errorHandler = new ErrorHandler(ConfigFactory.create(ErrorConfig.class, config));
        Sink baseSink = sinkFactory.getSink();
        Sink sinkWithFailHandler = new SinkWithFailHandler(baseSink, errorHandler);
        Sink sinkWithRetry = withRetry(sinkWithFailHandler, errorHandler);
        Sink sinkWithDLQ = withDlq(sinkWithRetry, tracer, errorHandler);
        return new SinkFinal(sinkWithDLQ, new FirehoseInstrumentation(statsDReporter, SinkFinal.class));
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
                new FirehoseInstrumentation(statsDReporter, SinkWithDlq.class));
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
        return new SinkWithRetry(sink, backOffProvider, new FirehoseInstrumentation(statsDReporter, SinkWithRetry.class), appConfig, parser, errorHandler);
    }

    private BackOffProvider getBackOffProvider() {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, config);
        return new ExponentialBackOffProvider(
                appConfig.getRetryExponentialBackoffInitialMs(),
                appConfig.getRetryExponentialBackoffRate(),
                appConfig.getRetryExponentialBackoffMaxMs(),
                new FirehoseInstrumentation(statsDReporter, ExponentialBackOffProvider.class),
                new BackOff(new FirehoseInstrumentation(statsDReporter, BackOff.class)));
    }
}
