package com.gojek.esb.factory;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.config.ExponentialBackOffProviderConfig;
import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.config.RetryQueueConfig;
import com.gojek.esb.consumer.EsbGenericConsumer;
import com.gojek.esb.consumer.FireHoseConsumer;
import com.gojek.esb.exception.EglcConfigurationException;
import com.gojek.esb.filter.EsbMessageFilter;
import com.gojek.esb.filter.Filter;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.metrics.StatsDReporterFactory;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.db.DBSinkFactory;
import com.gojek.esb.sink.elasticsearch.ESSinkFactory;
import com.gojek.esb.sink.grpc.GrpcSinkFactory;
import com.gojek.esb.sink.http.HttpSinkFactory;
import com.gojek.esb.sink.influxdb.InfluxSinkFactory;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class FireHoseConsumerFactory {

    private Map<String, String> config = System.getenv();
    private final KafkaConsumerConfig kafkaConsumerConfig;
    private final StatsDReporter statsDReporter;
    private final Clock clockInstance;
    private static final Logger LOGGER = LoggerFactory.getLogger(FireHoseConsumerFactory.class);
    private StencilClient stencilClient;

    public FireHoseConsumerFactory(KafkaConsumerConfig kafkaConsumerConfig) {
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        LOGGER.info("--------- Config ---------");
        LOGGER.info(this.kafkaConsumerConfig.getKafkaAddress());
        LOGGER.info(this.kafkaConsumerConfig.getKafkaTopic());
        LOGGER.info(this.kafkaConsumerConfig.getConsumerGroupId());
        LOGGER.info("--------- ------ ---------");
        clockInstance = new Clock();
        StatsDReporterFactory statsDReporterFactory = new StatsDReporterFactory(
                this.kafkaConsumerConfig.getStatsDHost(),
                this.kafkaConsumerConfig.getStatsDPort(),
                this.kafkaConsumerConfig.getStatsDTags().split(",")
        );
        statsDReporter = statsDReporterFactory.buildReporter();

        String stencilUrl = this.kafkaConsumerConfig.stencilUrl();
        stencilClient = this.kafkaConsumerConfig.enableStencilClient()
                ? StencilClientFactory.getClient(stencilUrl, config, statsDReporterFactory.getStatsDClient())
                : StencilClientFactory.getClient();
    }

    /**
     * Helps to create consumer based on the config.
     *
     * @return FireHoseConsumer
     */
    public FireHoseConsumer buildConsumer() {

        Filter filter = new EsbMessageFilter(kafkaConsumerConfig);
        GenericKafkaFactory genericKafkaFactory = new GenericKafkaFactory();
        Tracer tracer = NoopTracerFactory.create();
        if (kafkaConsumerConfig.enableTracing()) {
            tracer = Configuration.fromEnv("FireHose" + ": " + kafkaConsumerConfig.getConsumerGroupId()).getTracer();
        }
        EsbGenericConsumer consumer = genericKafkaFactory.createConsumer(kafkaConsumerConfig, config,
                statsDReporter, filter, tracer);
        Sink retrySink = withRetry(getSink(), genericKafkaFactory, tracer);
        SinkTracer fireHoseTracer = new SinkTracer(tracer, kafkaConsumerConfig.getSinkType().name() + " SINK",
                kafkaConsumerConfig.enableTracing());
        return new FireHoseConsumer(consumer, retrySink, statsDReporter, clockInstance, fireHoseTracer);
    }

    /**
     * return the basic Sink implementation based on the config.
     *
     * @return Sink
     */
    private Sink getSink() {
        switch (kafkaConsumerConfig.getSinkType()) {
            case DB:
                return new DBSinkFactory().create(config, statsDReporter, stencilClient);
            case HTTP:
                return new HttpSinkFactory().create(config, statsDReporter, stencilClient);
            case INFLUXDB:
                return new InfluxSinkFactory().create(config, statsDReporter, stencilClient);
            case LOG:
                return new LogSinkFactory().create(config, statsDReporter, stencilClient);
//            case CLEVERTAP:
//                return new ClevertapSinkFactory().create(config, statsDReporter, stencilClient);
            case ELASTICSEARCH:
                return new ESSinkFactory().create(config, statsDReporter, stencilClient);
            case REDIS:
                return new RedisSinkFactory().create(config, statsDReporter, stencilClient);
            case GRPC:
                return new GrpcSinkFactory().create(config, statsDReporter, stencilClient);
            default:
                throw new EglcConfigurationException("Invalid FireHose SINK type");

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
                backOffConfig.exponentialBackoffInitialTimeInMs(),
                backOffConfig.exponentialBackoffRate(),
                backOffConfig.exponentialBackoffMaximumBackoffInMs(),
                statsDReporter,
                BackOff.withInstrumentationFactory(statsDReporter));

        if (kafkaConsumerConfig.getRetryQueueEnabled()) {
            RetryQueueConfig retryQueueConfig = ConfigFactory.create(RetryQueueConfig.class, config);
            KafkaProducer<byte[], byte[]> kafkaProducer = genericKafkaFactory.getKafkaProducer(retryQueueConfig);
            TracingKafkaProducer<byte[], byte[]> tracingProducer = new TracingKafkaProducer<>(kafkaProducer, tracer);

            return SinkWithRetryQueue.withInstrumentationFactory(
                    new SinkWithRetry(basicSink, backOffProvider, statsDReporter,
                            kafkaConsumerConfig.getMaximumRetryAttempts()),
                    tracingProducer, retryQueueConfig.getRetryTopic(), statsDReporter, backOffProvider);
        } else {
            return new SinkWithRetry(basicSink, backOffProvider, statsDReporter);
        }
    }
}
