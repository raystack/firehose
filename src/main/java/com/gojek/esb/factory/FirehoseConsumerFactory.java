package com.gojek.esb.factory;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.ExponentialBackOffProviderConfig;
import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.config.RetryQueueConfig;
import com.gojek.esb.consumer.GenericConsumer;
import com.gojek.esb.consumer.FirehoseConsumer;
import com.gojek.esb.exception.EglcConfigurationException;
import com.gojek.esb.filter.MessageFilter;
import com.gojek.esb.filter.Filter;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.jdbc.JdbcSinkFactory;
import com.gojek.esb.sink.elasticsearch.EsSinkFactory;
import com.gojek.esb.sink.grpc.GrpcSinkFactory;
import com.gojek.esb.sink.http.HttpSinkFactory;
import com.gojek.esb.sink.influxdb.InfluxSinkFactory;
import com.gojek.esb.sink.log.KeyOrMessageParser;
import com.gojek.esb.sink.log.LogSinkFactory;
import com.gojek.esb.sink.redis.RedisSinkFactory;
import com.gojek.esb.sinkdecorator.BackOff;
import com.gojek.esb.sinkdecorator.BackOffProvider;
import com.gojek.esb.sinkdecorator.ExponentialBackOffProvider;
import com.gojek.esb.sinkdecorator.SinkWithRetry;
import com.gojek.esb.sinkdecorator.SinkWithRetryQueue;
import com.gojek.esb.tracer.SinkTracer;
import com.gojek.esb.util.Clock;
import io.jaegertracing.Configuration;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaProducer;
import io.opentracing.noop.NoopTracerFactory;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Map;

public class FirehoseConsumerFactory {

    private Map<String, String> config = System.getenv();
    private final KafkaConsumerConfig kafkaConsumerConfig;
    private StatsDReporter statsDReporter;
    private final Clock clockInstance;
    private StencilClient stencilClient;
    private Instrumentation instrumentation;
    private KeyOrMessageParser parser;

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

        clockInstance = new Clock();

        String stencilUrl = this.kafkaConsumerConfig.getStencilUrls();
        stencilClient = this.kafkaConsumerConfig.isStencilEnable()
                ? StencilClientFactory.getClient(stencilUrl, config, this.statsDReporter.getClient())
                : StencilClientFactory.getClient();
        parser = new KeyOrMessageParser(new ProtoParser(stencilClient, kafkaConsumerConfig.getProtoSchema()), kafkaConsumerConfig);
    }

    /**
     * Helps to create consumer based on the config.
     *
     * @return FirehoseConsumer
     */
    public FirehoseConsumer buildConsumer() {

        Filter filter = new MessageFilter(kafkaConsumerConfig, new Instrumentation(statsDReporter, MessageFilter.class));
        GenericKafkaFactory genericKafkaFactory = new GenericKafkaFactory();
        Tracer tracer = NoopTracerFactory.create();
        if (kafkaConsumerConfig.isTraceEnable()) {
            tracer = Configuration.fromEnv("Firehose" + ": " + kafkaConsumerConfig.getSourceKafkaConsumerGroupId()).getTracer();
        }
        GenericConsumer consumer = genericKafkaFactory.createConsumer(kafkaConsumerConfig, config,
                statsDReporter, filter, tracer);
        Sink retrySink = withRetry(getSink(), genericKafkaFactory, tracer);
        SinkTracer firehoseTracer = new SinkTracer(tracer, kafkaConsumerConfig.getSinkType().name() + " SINK",
                kafkaConsumerConfig.isTraceEnable());
        return new FirehoseConsumer(consumer, retrySink, clockInstance, firehoseTracer, new Instrumentation(statsDReporter, FirehoseConsumer.class));
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
                return new JdbcSinkFactory().create(config, statsDReporter, stencilClient);
            case HTTP:
                return new HttpSinkFactory().create(config, statsDReporter, stencilClient);
            case INFLUXDB:
                return new InfluxSinkFactory().create(config, statsDReporter, stencilClient);
            case LOG:
                return new LogSinkFactory().create(config, statsDReporter, stencilClient);
            case ELASTICSEARCH:
                return new EsSinkFactory().create(config, statsDReporter, stencilClient);
            case REDIS:
                return new RedisSinkFactory().create(config, statsDReporter, stencilClient);
            case GRPC:
                return new GrpcSinkFactory().create(config, statsDReporter, stencilClient);
            default:
                throw new EglcConfigurationException("Invalid Firehose sink.type");

        }
    }

    /**
     * to enable the retry feature for the basic sinks based on the config.
     *
     * @param basicSink
     * @param genericKafkaFactory
     * @return Sink
     */
    private Sink withRetry(Sink basicSink, GenericKafkaFactory genericKafkaFactory, Tracer tracer) {
        ExponentialBackOffProviderConfig backOffConfig = ConfigFactory.create(ExponentialBackOffProviderConfig.class,
                config);
        BackOffProvider backOffProvider = new ExponentialBackOffProvider(
                backOffConfig.getRetryExponentialBackoffInitialMs(),
                backOffConfig.getRetryExponentialBackoffRate(),
                backOffConfig.getRetryExponentialBackoffMaxMs(),
                new Instrumentation(statsDReporter, ExponentialBackOffProvider.class),
                new BackOff(new Instrumentation(statsDReporter, BackOff.class)));

        if (kafkaConsumerConfig.isRetryQueueEnable()) {
            RetryQueueConfig retryQueueConfig = ConfigFactory.create(RetryQueueConfig.class, config);

            KafkaProducer<byte[], byte[]> kafkaProducer = genericKafkaFactory.getKafkaProducer(retryQueueConfig);
            TracingKafkaProducer<byte[], byte[]> tracingProducer = new TracingKafkaProducer<>(kafkaProducer, tracer);

            return SinkWithRetryQueue.withInstrumentationFactory(
                    new SinkWithRetry(basicSink, backOffProvider, new Instrumentation(statsDReporter, SinkWithRetry.class),
                            kafkaConsumerConfig.getRetryQueueAttemptsToTrigger(), parser),
                    tracingProducer, retryQueueConfig.getRetryQueueKafkaTopic(), statsDReporter, backOffProvider);
        } else {
            return new SinkWithRetry(basicSink, backOffProvider, new Instrumentation(statsDReporter, SinkWithRetry.class), parser);
        }
    }
}
