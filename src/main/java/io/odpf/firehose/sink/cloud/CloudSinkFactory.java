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
import io.odpf.firehose.sink.cloud.writer.path.TimePartitionPath;
import io.odpf.firehose.sink.cloud.writer.policy.SizeBasedRotatingPolicy;
import io.odpf.firehose.sink.cloud.writer.policy.TimeBasedRotatingPolicy;
import io.odpf.firehose.sink.cloud.writer.policy.WriterPolicy;
import org.aeonbits.owner.ConfigFactory;

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

        Constants.LocalFileWriterType writerType = Constants.LocalFileWriterType.valueOf(sinkConfig.getLocalFileWriterType());
        Path basePath = Paths.get(sinkConfig.getLocalDirectory());
        ProtoParser protoParser = new ProtoParser(stencilClient, sinkConfig.getInputSchemaProtoClass());
        KafkaMetadataUtils kafkaMetadataUtils = new KafkaMetadataUtils(sinkConfig.getKafkaMetadataColumnName());
        MessageSerializer messageSerializer = new MessageSerializer(kafkaMetadataUtils, sinkConfig.getWriteKafkaMetadata(), protoParser);
        WriterOrchestrator writerOrchestrator = new WriterOrchestrator(sinkConfig.getWriterBlockSize(),
                sinkConfig.getWriterPageSize(),
                messageDescriptor,
                metadataMessageDescriptor.getFields(),
                writerPolicies,
                timePartitionPath,
                writerType,
                basePath,
                messageSerializer);

        return new CloudSink(new Instrumentation(statsDReporter, CloudSink.class), "file", writerOrchestrator);
    }
}
