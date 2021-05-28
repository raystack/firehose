package io.odpf.firehose.sink.file;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.google.protobuf.Descriptors;
import io.odpf.firehose.config.FileSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sink.SinkFactory;
import io.odpf.firehose.sink.file.message.KafkaMetadataUtils;
import io.odpf.firehose.sink.file.message.MessageSerializer;
import io.odpf.firehose.sink.file.proto.KafkaMetadataProto;
import io.odpf.firehose.sink.file.proto.KafkaMetadataProtoFile;
import io.odpf.firehose.sink.file.proto.NestedKafkaMetadataProto;
import io.odpf.firehose.sink.file.writer.PartitioningWriter;
import io.odpf.firehose.sink.file.writer.path.TimePartitionPath;
import io.odpf.firehose.sink.file.writer.policy.SizeBasedRotatingPolicy;
import io.odpf.firehose.sink.file.writer.policy.TimeBasedRotatingPolicy;
import io.odpf.firehose.sink.file.writer.policy.WriterPolicy;
import org.aeonbits.owner.ConfigFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileSinkFactory implements SinkFactory {
    @Override
    public Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        FileSinkConfig sinkConfig = ConfigFactory.create(FileSinkConfig.class, configuration);

        Instrumentation instrumentation = new Instrumentation(statsDReporter, FileSinkFactory.class);

        Descriptors.Descriptor messageDescriptor = stencilClient.get(sinkConfig.getInputSchemaProtoClass());

        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoFile.createFileDescriptor(sinkConfig.getKafkaMetadataColumnName());
        Descriptors.Descriptor metadataMessageDescriptor;
        if (sinkConfig.getKafkaMetadataColumnName().isEmpty()) {
            metadataMessageDescriptor = fileDescriptor.findMessageTypeByName(KafkaMetadataProto.getTypeName());
        }else{
            metadataMessageDescriptor = fileDescriptor.findMessageTypeByName(NestedKafkaMetadataProto.getTypeName());
        }

        List<WriterPolicy> writerPolicies = new ArrayList<>();
        if(sinkConfig.getFileRotationDurationMillis() != -1){
            TimeBasedRotatingPolicy rotatingPolicy = new TimeBasedRotatingPolicy(sinkConfig.getFileRotationDurationMillis());
            writerPolicies.add(rotatingPolicy);
        }

        if(sinkConfig.getFileRationMaxSizeBytes() != -1){
            SizeBasedRotatingPolicy rotatingPolicy = new SizeBasedRotatingPolicy(sinkConfig.getFileRationMaxSizeBytes());
            writerPolicies.add(rotatingPolicy);
        }

        TimePartitionPath timePartitionPath = new TimePartitionPath(sinkConfig.getKafkaMetadataColumnName(),
                sinkConfig.getTimePartitioningFieldName(),
                sinkConfig.getTimePartitioningDatePattern(),
                sinkConfig.getTimePartitioningTimeZone(),
                sinkConfig.getTimePartitioningDatePrefix());

        PartitioningWriter partitioningWriter = new PartitioningWriter(sinkConfig.getWriterBlockSize(),
                sinkConfig.getWriterPageSize(),
                messageDescriptor,
                metadataMessageDescriptor.getFields(),
                writerPolicies,
                timePartitionPath);

        ProtoParser protoParser = new ProtoParser(stencilClient, sinkConfig.getInputSchemaProtoClass());
        KafkaMetadataUtils kafkaMetadataUtils = new KafkaMetadataUtils(sinkConfig.getKafkaMetadataColumnName());
        MessageSerializer messageSerializer = new MessageSerializer(kafkaMetadataUtils, sinkConfig.getWriteKafkaMetadata(), protoParser);

        Path basePath = Paths.get(sinkConfig.getLocalDirectory());

        return new FileSink(new Instrumentation(statsDReporter, FileSink.class), "file", partitioningWriter, messageSerializer, basePath);
    }
}
