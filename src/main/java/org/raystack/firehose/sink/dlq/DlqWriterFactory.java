package org.raystack.firehose.sink.dlq;

import org.raystack.firehose.config.DlqConfig;
import org.raystack.firehose.config.DlqKafkaProducerConfig;
import org.raystack.firehose.metrics.FirehoseInstrumentation;
import org.raystack.firehose.sink.common.blobstorage.BlobStorage;
import org.raystack.firehose.sink.common.blobstorage.BlobStorageFactory;
import org.raystack.firehose.sink.dlq.blobstorage.BlobStorageDlqWriter;
import org.raystack.firehose.sink.dlq.kafka.KafkaDlqWriter;
import org.raystack.firehose.sink.dlq.log.LogDlqWriter;
import org.raystack.firehose.utils.KafkaUtils;
import org.raystack.depot.metrics.StatsDReporter;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaProducer;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Map;

public class DlqWriterFactory {

    public static DlqWriter create(Map<String, String> configuration, StatsDReporter client, Tracer tracer) {
        DlqConfig dlqConfig = ConfigFactory.create(DlqConfig.class, configuration);

        switch (dlqConfig.getDlqWriterType()) {
            case KAFKA:
                DlqKafkaProducerConfig dlqKafkaProducerConfig = ConfigFactory.create(DlqKafkaProducerConfig.class, configuration);
                KafkaProducer<byte[], byte[]> kafkaProducer = KafkaUtils.getKafkaProducer(dlqKafkaProducerConfig);
                TracingKafkaProducer<byte[], byte[]> tracingProducer = new TracingKafkaProducer<>(kafkaProducer, tracer);

                return new KafkaDlqWriter(tracingProducer, dlqKafkaProducerConfig.getDlqKafkaTopic(), new FirehoseInstrumentation(client, KafkaDlqWriter.class));

            case BLOB_STORAGE:
                switch (dlqConfig.getBlobStorageType()) {
                    case GCS:
                        configuration.put("GCS_TYPE", "DLQ");
                        break;
                    case S3:
                        configuration.put("S3_TYPE", "DLQ");
                        break;
                    default:
                        throw new IllegalArgumentException("DLQ Blob Storage type " + dlqConfig.getBlobStorageType() + "is not supported");
                }
                BlobStorage blobStorage = BlobStorageFactory.createObjectStorage(dlqConfig.getBlobStorageType(), configuration);
                return new BlobStorageDlqWriter(blobStorage);
            case LOG:
                return new LogDlqWriter(new FirehoseInstrumentation(client, LogDlqWriter.class));

            default:
                throw new IllegalArgumentException("DLQ Writer type " + dlqConfig.getDlqWriterType() + " is not supported");
        }
    }
}
