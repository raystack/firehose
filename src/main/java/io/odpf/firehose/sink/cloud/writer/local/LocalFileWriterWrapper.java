package io.odpf.firehose.sink.cloud.writer.local;

import com.google.protobuf.Descriptors;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

@AllArgsConstructor
public class LocalFileWriterWrapper {
    private final Constructor<LocalFileWriter> localFileWriterConstructor;
    private final int pageSize;
    private final int blockSize;
    private final Descriptors.Descriptor messageDescriptor;
    private final List<Descriptors.FieldDescriptor> metadataFieldDescriptor;


    public LocalFileWriter createLocalFileWriter(Path basePath, Path partitionedPath) throws IOException {
        String fileName = UUID.randomUUID().toString();
        Path dir = basePath.resolve(partitionedPath);
        Path fullPath = dir.resolve(Paths.get(fileName));
        try {
            return localFileWriterConstructor.newInstance(System.currentTimeMillis(), fullPath.toString(), pageSize, blockSize, messageDescriptor, metadataFieldDescriptor);
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new IOException(e);
        }

    }
}
