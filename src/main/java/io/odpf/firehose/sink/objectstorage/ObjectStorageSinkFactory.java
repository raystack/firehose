package io.odpf.firehose.sink.objectstorage;

import com.gojek.de.stencil.client.StencilClient;
import com.google.protobuf.Descriptors;
import io.odpf.firehose.config.ObjectStorageSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.objectstorage.ObjectStorageFactory;
import io.odpf.firehose.objectstorage.ObjectStorageType;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sink.SinkFactory;
import io.odpf.firehose.sink.objectstorage.message.MessageDeSerializer;
import io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataProtoMessage;
import io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataProtoMessageUtils;
import io.odpf.firehose.sink.objectstorage.proto.NestedKafkaMetadataProtoMessage;
import io.odpf.firehose.sink.objectstorage.writer.WriterOrchestrator;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalStorage;
import io.odpf.firehose.sink.objectstorage.writer.local.policy.SizeBasedRotatingPolicy;
import io.odpf.firehose.sink.objectstorage.writer.local.policy.TimeBasedRotatingPolicy;
import io.odpf.firehose.sink.objectstorage.writer.local.policy.WriterPolicy;
import org.aeonbits.owner.ConfigFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ObjectStorageSinkFactory implements SinkFactory {

    @Override
    public Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        ObjectStorageSinkConfig sinkConfig = ConfigFactory.create(ObjectStorageSinkConfig.class, configuration);

        LocalStorage localStorage = getLocalFileWriterWrapper(sinkConfig, stencilClient, statsDReporter);

        ObjectStorage sinkObjectStorage = createSinkObjectStorage(sinkConfig, new HashMap<>(configuration));

        WriterOrchestrator writerOrchestrator = new WriterOrchestrator(sinkConfig, localStorage, sinkObjectStorage, statsDReporter);
        MessageDeSerializer messageDeSerializer = new MessageDeSerializer(sinkConfig, stencilClient);

        return new ObjectStorageSink(new Instrumentation(statsDReporter, ObjectStorageSink.class), sinkConfig.getSinkType().toString(), writerOrchestrator, messageDeSerializer);
    }

    private Descriptors.Descriptor getMetadataMessageDescriptor(ObjectStorageSinkConfig sinkConfig) {
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoMessageUtils.createFileDescriptor(sinkConfig.getKafkaMetadataColumnName());
        return sinkConfig.getKafkaMetadataColumnName().isEmpty()
                ? fileDescriptor.findMessageTypeByName(KafkaMetadataProtoMessage.getTypeName())
                : fileDescriptor.findMessageTypeByName(NestedKafkaMetadataProtoMessage.getTypeName());

    }

    private LocalStorage getLocalFileWriterWrapper(ObjectStorageSinkConfig sinkConfig, StencilClient stencilClient, StatsDReporter statsDReporter) {
        Descriptors.Descriptor outputMessageDescriptor = stencilClient.get(sinkConfig.getInputSchemaProtoClass());
        Descriptors.Descriptor metadataMessageDescriptor = getMetadataMessageDescriptor(sinkConfig);

        List<WriterPolicy> writerPolicies = new ArrayList<>();
        writerPolicies.add(new TimeBasedRotatingPolicy(sinkConfig.getFileRotationDurationMS()));
        writerPolicies.add(new SizeBasedRotatingPolicy(sinkConfig.getFileRotationMaxSizeBytes()));

        return new LocalStorage(
                sinkConfig,
                outputMessageDescriptor,
                metadataMessageDescriptor.getFields(),
                writerPolicies,
                new Instrumentation(statsDReporter, LocalStorage.class));
    }

    public ObjectStorage createSinkObjectStorage(ObjectStorageSinkConfig sinkConfig, Map<String, String> configuration) {
        if (sinkConfig.getObjectStorageType() == ObjectStorageType.GCS) {
            configuration.put("GCS_TYPE", "SINK_OBJECT_STORAGE");
            return ObjectStorageFactory.createObjectStorage(sinkConfig.getObjectStorageType(), configuration);
        }
        throw new IllegalArgumentException("Sink Object Storage type " + sinkConfig.getObjectStorageType() + "is not supported");
    }
}
