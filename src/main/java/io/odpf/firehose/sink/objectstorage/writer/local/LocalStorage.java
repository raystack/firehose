package io.odpf.firehose.sink.objectstorage.writer.local;

import com.google.protobuf.Descriptors;
import io.odpf.firehose.config.ObjectStorageSinkConfig;
import io.odpf.firehose.exception.ConfigurationException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.objectstorage.writer.local.policy.WriterPolicy;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

@AllArgsConstructor
public class LocalStorage {

    private final ObjectStorageSinkConfig sinkConfig;
    private final Descriptors.Descriptor messageDescriptor;
    private final List<Descriptors.FieldDescriptor> metadataFieldDescriptor;
    private final List<WriterPolicy> policies;
    private final Instrumentation instrumentation;

    public LocalFileWriter createLocalFileWriter(Path partitionPath) {
        Path basePath = Paths.get(sinkConfig.getLocalDirectory());
        String fileName = UUID.randomUUID().toString();
        Path dir = basePath.resolve(partitionPath);
        Path fullPath = dir.resolve(Paths.get(fileName));
        return createWriter(basePath, fullPath);
    }

    private LocalParquetFileWriter createWriter(Path basePath, Path fullPath) {
        switch (sinkConfig.getFileWriterType()) {
            case PARQUET:
                try {
                    instrumentation.logInfo("Creating Local File " + fullPath);
                    return new LocalParquetFileWriter(
                            System.currentTimeMillis(),
                            basePath.toString(),
                            fullPath.toString(),
                            sinkConfig.getWriterPageSize(),
                            sinkConfig.getWriterBlockSize(),
                            messageDescriptor,
                            metadataFieldDescriptor);
                } catch (IOException e) {
                    throw new LocalFileWriterFailedException(e);
                }
            default:
                throw new ConfigurationException("unsupported file writer type");
        }
    }

    public void deleteLocalFile(String pathString) {
        try {
            instrumentation.logInfo("Deleting Local File " + pathString);
            Files.delete(Paths.get(pathString));
        } catch (IOException e) {
            throw new LocalFileWriterFailedException(e);
        }
    }

    public Boolean shouldRotate(LocalFileWriter writer) {
        return policies.stream().anyMatch(writerPolicy -> writerPolicy.shouldRotate(writer.getMetadata()));
    }
}
