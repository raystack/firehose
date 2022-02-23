package io.odpf.firehose.sink.blob;

import com.google.protobuf.Descriptors;
import io.odpf.firehose.config.BlobSinkConfig;
import io.odpf.firehose.consumer.kafka.OffsetManager;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sink.blob.message.MessageDeSerializer;
import io.odpf.firehose.sink.blob.proto.KafkaMetadataProtoMessage;
import io.odpf.firehose.sink.blob.proto.KafkaMetadataProtoMessageUtils;
import io.odpf.firehose.sink.blob.proto.NestedKafkaMetadataProtoMessage;
import io.odpf.firehose.sink.blob.writer.WriterOrchestrator;
import io.odpf.firehose.sink.blob.writer.local.LocalStorage;
import io.odpf.firehose.sink.blob.writer.local.policy.SizeBasedRotatingPolicy;
import io.odpf.firehose.sink.blob.writer.local.policy.TimeBasedRotatingPolicy;
import io.odpf.firehose.sink.blob.writer.local.policy.WriterPolicy;
import io.odpf.firehose.sink.common.blobstorage.BlobStorage;
import io.odpf.firehose.sink.common.blobstorage.BlobStorageFactory;
import io.odpf.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlobSinkFactory {

    public static Sink create(Map<String, String> configuration, OffsetManager offsetManager, StatsDReporter statsDReporter, StencilClient stencilClient) {
        BlobSinkConfig sinkConfig = ConfigFactory.create(BlobSinkConfig.class, configuration);
        LocalStorage localStorage = getLocalFileWriterWrapper(sinkConfig, stencilClient, statsDReporter);
        BlobStorage sinkBlobStorage = createSinkObjectStorage(sinkConfig, new HashMap<>(configuration));
        WriterOrchestrator writerOrchestrator = new WriterOrchestrator(sinkConfig, localStorage, sinkBlobStorage, statsDReporter);
        MessageDeSerializer messageDeSerializer = new MessageDeSerializer(sinkConfig, stencilClient);
        return new BlobSink(
                new Instrumentation(statsDReporter, BlobSink.class),
                sinkConfig.getSinkType().toString(),
                offsetManager,
                writerOrchestrator,
                messageDeSerializer);
    }

    private static Descriptors.Descriptor getMetadataMessageDescriptor(BlobSinkConfig sinkConfig) {
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoMessageUtils.createFileDescriptor(sinkConfig.getOutputKafkaMetadataColumnName());
        return sinkConfig.getOutputKafkaMetadataColumnName().isEmpty()
                ? fileDescriptor.findMessageTypeByName(KafkaMetadataProtoMessage.getTypeName())
                : fileDescriptor.findMessageTypeByName(NestedKafkaMetadataProtoMessage.getTypeName());

    }

    private static LocalStorage getLocalFileWriterWrapper(BlobSinkConfig sinkConfig, StencilClient stencilClient, StatsDReporter statsDReporter) {
        Descriptors.Descriptor outputMessageDescriptor = stencilClient.get(sinkConfig.getInputSchemaProtoClass());
        Descriptors.Descriptor metadataMessageDescriptor = getMetadataMessageDescriptor(sinkConfig);
        List<WriterPolicy> writerPolicies = new ArrayList<>();
        writerPolicies.add(new TimeBasedRotatingPolicy(sinkConfig.getLocalFileRotationDurationMS()));
        writerPolicies.add(new SizeBasedRotatingPolicy(sinkConfig.getLocalFileRotationMaxSizeBytes()));
        return new LocalStorage(
                sinkConfig,
                outputMessageDescriptor,
                metadataMessageDescriptor.getFields(),
                writerPolicies,
                new Instrumentation(statsDReporter, LocalStorage.class));
    }

    public static BlobStorage createSinkObjectStorage(BlobSinkConfig sinkConfig, Map<String, String> configuration) {
        switch (sinkConfig.getBlobStorageType()) {
            case GCS:
                configuration.put("GCS_TYPE", "SINK_BLOB");
                return BlobStorageFactory.createObjectStorage(sinkConfig.getBlobStorageType(), configuration);
            case S3:
                configuration.put("S3_TYPE", "SINK_BLOB");
                return BlobStorageFactory.createObjectStorage(sinkConfig.getBlobStorageType(), configuration);
            default:
                throw new IllegalArgumentException("Sink Blob Storage type " + sinkConfig.getBlobStorageType() + "is not supported");
        }

    }
}
