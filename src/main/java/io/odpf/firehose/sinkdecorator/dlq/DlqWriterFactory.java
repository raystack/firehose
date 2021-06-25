package io.odpf.firehose.sinkdecorator.dlq;

import io.odpf.firehose.config.AppConfig;
import io.odpf.firehose.config.DlqConfig;
import io.odpf.firehose.factory.GenericKafkaFactory;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.objectstorage.ObjectStorageFactory;
import io.odpf.firehose.objectstorage.ObjectStorageType;
import io.odpf.firehose.objectstorage.gcs.GCSConfig;
import io.odpf.firehose.sinkdecorator.BackOff;
import io.odpf.firehose.sinkdecorator.BackOffProvider;
import io.odpf.firehose.sinkdecorator.ExponentialBackOffProvider;
import io.odpf.firehose.sinkdecorator.dlq.kafka.KafkaDlqWriter;
import io.odpf.firehose.sinkdecorator.dlq.objectstorage.ObjectStorageDlqWriter;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaProducer;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.nio.file.Paths;
import java.util.Map;

public class DlqWriterFactory {
    public DlqWriter create(Map<String, String> configuration, StatsDReporter client, Tracer tracer) {
        DlqConfig dlqConfig = ConfigFactory.create(DlqConfig.class, configuration);

        switch (dlqConfig.getDlqWriterType()) {
            case KAFKA:
                AppConfig appConfig = ConfigFactory.create(AppConfig.class,
                        configuration);
                BackOffProvider backOffProvider = new ExponentialBackOffProvider(
                        appConfig.getRetryExponentialBackoffInitialMs(),
                        appConfig.getRetryExponentialBackoffRate(),
                        appConfig.getRetryExponentialBackoffMaxMs(),
                        new Instrumentation(client, ExponentialBackOffProvider.class),
                        new BackOff(new Instrumentation(client, BackOff.class)));

                GenericKafkaFactory genericKafkaFactory = new GenericKafkaFactory();
                KafkaProducer<byte[], byte[]> kafkaProducer = genericKafkaFactory.getKafkaProducer(dlqConfig);
                TracingKafkaProducer<byte[], byte[]> tracingProducer = new TracingKafkaProducer<>(kafkaProducer, tracer);

                return new KafkaDlqWriter(tracingProducer, dlqConfig.getDlqKafkaTopic(), backOffProvider, new Instrumentation(client, KafkaDlqWriter.class));

            case OBJECTSTORAGE:
                GCSConfig gcsConfig = new GCSConfig(Paths.get(""),
                        dlqConfig.getDlqObjectStorageBucketName(),
                        dlqConfig.getDlqGCSCredentialPath(),
                        dlqConfig.getDlqGcsGcloudProjectID());

                ObjectStorage objectStorage = ObjectStorageFactory.createObjectStorage(ObjectStorageType.GCS, gcsConfig);
                return new ObjectStorageDlqWriter(objectStorage);

            case LOG:
                return new LogDlqWriter(new Instrumentation(client, LogDlqWriter.class));

            default:
                throw new IllegalArgumentException("DLQ Writer type " + dlqConfig.getDlqWriterType() + " is not supported");
        }
    }
}
