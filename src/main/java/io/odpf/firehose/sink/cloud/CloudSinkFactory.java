package io.odpf.firehose.sink.cloud;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.google.protobuf.Descriptors;
import io.odpf.firehose.config.CloudSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sink.SinkFactory;
import io.odpf.firehose.sink.cloud.message.KafkaMetadataUtils;
import io.odpf.firehose.sink.cloud.message.MessageSerializer;
import io.odpf.firehose.sink.cloud.proto.KafkaMetadataProto;
import io.odpf.firehose.sink.cloud.proto.KafkaMetadataProtoFile;
import io.odpf.firehose.sink.cloud.proto.NestedKafkaMetadataProto;
import io.odpf.firehose.sink.cloud.writer.WriterOrchestrator;
import io.odpf.firehose.sink.cloud.writer.local.LocalFileWriter;
import io.odpf.firehose.sink.cloud.writer.local.LocalFileWriterWrapper;
import io.odpf.firehose.sink.cloud.writer.local.TimePartitionPath;
import io.odpf.firehose.sink.cloud.writer.local.policy.SizeBasedRotatingPolicy;
import io.odpf.firehose.sink.cloud.writer.local.policy.TimeBasedRotatingPolicy;
import io.odpf.firehose.sink.cloud.writer.local.policy.WriterPolicy;
import org.aeonbits.owner.ConfigFactory;

import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CloudSinkFactory implements SinkFactory {
    @Override
    public Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        CloudSinkConfig sinkConfig = ConfigFactory.create(CloudSinkConfig.class, configuration);

        Instrumentation instrumentation = new Instrumentation(statsDReporter, CloudSinkFactory.class);

        Descriptors.Descriptor messageDescriptor = stencilClient.get(sinkConfig.getInputSchemaProtoClass());

        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoFile.createFileDescriptor(sinkConfig.getKafkaMetadataColumnName());
        Descriptors.Descriptor metadataMessageDescriptor;
        if (sinkConfig.getKafkaMetadataColumnName().isEmpty()) {
            metadataMessageDescriptor = fileDescriptor.findMessageTypeByName(KafkaMetadataProto.getTypeName());
        } else {
            metadataMessageDescriptor = fileDescriptor.findMessageTypeByName(NestedKafkaMetadataProto.getTypeName());
        }

        List<WriterPolicy> writerPolicies = new ArrayList<>();

        writerPolicies.add(new TimeBasedRotatingPolicy(sinkConfig.getFileRotationDurationMillis()));
        writerPolicies.add(new SizeBasedRotatingPolicy(sinkConfig.getFileRationMaxSizeBytes()));

        TimePartitionPath timePartitionPath = new TimePartitionPath(
                sinkConfig.getKafkaMetadataColumnName(),
                sinkConfig.getTimePartitioningFieldName(),
                sinkConfig.getTimePartitioningDatePattern(),
                sinkConfig.getPartitioningType(),
                sinkConfig.getTimePartitioningTimeZone(),
                sinkConfig.getTimePartitioningDatePrefix(),
                sinkConfig.getTimePartitioningHourPrefix());

        String writerClass = sinkConfig.getLocalFileWriterClass();
        Constructor<LocalFileWriter> localFileWriterConstructor;
        try {
            localFileWriterConstructor = (Constructor<LocalFileWriter>) Class.forName(writerClass).getConstructor(long.class, String.class, int.class, int.class, Descriptors.Descriptor.class, List.class);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }
        Path basePath = Paths.get(sinkConfig.getLocalDirectory());
        ProtoParser protoParser = new ProtoParser(stencilClient, sinkConfig.getInputSchemaProtoClass());
        KafkaMetadataUtils kafkaMetadataUtils = new KafkaMetadataUtils(sinkConfig.getKafkaMetadataColumnName());
        MessageSerializer messageSerializer = new MessageSerializer(kafkaMetadataUtils, sinkConfig.getWriteKafkaMetadata(), protoParser);
        LocalFileWriterWrapper localFileWriterWrapper =
                new LocalFileWriterWrapper(
                        localFileWriterConstructor,
                        sinkConfig.getWriterPageSize(),
                        sinkConfig.getWriterBlockSize(),
                        messageDescriptor,
                        metadataMessageDescriptor.getFields());
        WriterOrchestrator writerOrchestrator =
                new WriterOrchestrator(
                        localFileWriterWrapper,
                        writerPolicies,
                        timePartitionPath,
                        basePath,
                        messageSerializer);

        return new CloudSink(new Instrumentation(statsDReporter, CloudSink.class), "file", writerOrchestrator);
    }
}
