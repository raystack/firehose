package com.gojek.esb.factory;

import java.util.Map;
import java.util.Properties;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.config.RetryQueueConfig;
import com.gojek.esb.consumer.EsbGenericConsumer;
import com.gojek.esb.consumer.Offsets;
import com.gojek.esb.consumer.TopicOffsets;
import com.gojek.esb.consumer.TopicPartitionOffsets;
import com.gojek.esb.filter.Filter;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaConsumer;

/**
 * A factory class to instantiate a kafka consumer.
 */
public class GenericKafkaFactory {

    /**
     * method to create the {@link EsbGenericConsumer} from the parameters supplied.
     *
     * @param config               {@see KafkaConsumerConfig}
     * @param extraKafkaParameters a map containing kafka configurations available as a key/value pair.
     * @param statsDReporter       {@see StatsDClient}
     * @param filter               {@see Filter}, {@see com.gojek.esb.filter.EsbMessageFilter}
     * @return {@see EsbGenericConsumer}
     */
    public EsbGenericConsumer createConsumer(KafkaConsumerConfig config, Map<String, String> extraKafkaParameters,
                                             StatsDReporter statsDReporter, Filter filter, Tracer tracer) {

        KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(FactoryUtil.getConfig(config, extraKafkaParameters));
        FactoryUtil.configureSubscription(config, kafkaConsumer, statsDReporter);
        Offsets offsets = !config.commitOnlyCurrentPartitions()
                ? new TopicOffsets(kafkaConsumer, config, new Instrumentation(statsDReporter, TopicOffsets.class))
                : new TopicPartitionOffsets(kafkaConsumer, config, new Instrumentation(statsDReporter, TopicPartitionOffsets.class));
        TracingKafkaConsumer<byte[], byte[]> tracingKafkaConsumer = new TracingKafkaConsumer<>(kafkaConsumer, tracer);
        return new EsbGenericConsumer(
            tracingKafkaConsumer,
            config,
            filter,
            offsets,
            new Instrumentation(statsDReporter, EsbGenericConsumer.class));
    }

    public KafkaProducer<byte[], byte[]> getKafkaProducer(RetryQueueConfig config) {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getRetryQueueKafkaBootStrapServers());
        props.put("acks", config.getRetryQueueKafkaAcks());
        props.put("retries", config.getRetryQueueKafkaRetries());
        props.put("batch.size", config.getRetryQueueKafkaBatchSize());
        props.put("linger.ms", config.getRetryQueueKafkaLingerSize());
        props.put("buffer.memory", config.getRetryQueueKafkaBufferMemory());
        props.put("key.serializer", config.getRetryQueueKafkaKeySerializer());
        props.put("value.serializer", config.getRetryQueueKafkaValueSerializer());

        return new KafkaProducer<>(props);
    }
}
