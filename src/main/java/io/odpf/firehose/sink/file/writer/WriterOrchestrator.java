package io.odpf.firehose.sink.file.writer;

import com.google.protobuf.Descriptors;
import io.odpf.firehose.sink.file.message.Record;
import io.odpf.firehose.sink.file.writer.path.TimePartitionPath;
import io.odpf.firehose.sink.file.writer.policy.WriterPolicy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class WriterOrchestrator implements Closeable {
    enum LocalWriterType {
        PARQUET,
        MEMORY
    }

    private LocalWriterType writerType;

    private int pageSize;
    private int blockSize;
    private Descriptors.Descriptor messageDescriptor;
    private List<Descriptors.FieldDescriptor> metadataFieldDescriptor;

    private List<WriterPolicy> policies;
    private TimePartitionPath timePartitionPath;

    private Map<Path, LocalFileWriter> writerMap = new ConcurrentHashMap<>();

    public WriterOrchestrator(int pageSize, int blockSize, Descriptors.Descriptor messageDescriptor, List<Descriptors.FieldDescriptor> metadataFieldDescriptor, List<WriterPolicy> policies, TimePartitionPath timePartitionPath) {
        this(pageSize, blockSize, messageDescriptor, metadataFieldDescriptor, policies, timePartitionPath, LocalWriterType.PARQUET);
    }

    public WriterOrchestrator(int pageSize, int blockSize, Descriptors.Descriptor messageDescriptor, List<Descriptors.FieldDescriptor> metadataFieldDescriptor, List<WriterPolicy> policies, TimePartitionPath timePartitionPath, LocalWriterType writerType) {
        this.pageSize = pageSize;
        this.blockSize = blockSize;
        this.messageDescriptor = messageDescriptor;
        this.metadataFieldDescriptor = metadataFieldDescriptor;
        this.policies = policies;
        this.timePartitionPath = timePartitionPath;
        this.writerType = writerType;
    }

    public LocalFileWriter getWriter(Path basePath, Record record) throws IOException {
        Path partitionedPath = timePartitionPath.create(record);

        if (writerMap.containsKey(partitionedPath)) {
            LocalFileWriter writer = writerMap.get(partitionedPath);

            if (!shouldRotate(writer)) {
                return writer;
            }

            writer.close();
        }

        String fileName = UUID.randomUUID().toString();
        Path dir = basePath.resolve(partitionedPath);
        Path fullPath = dir.resolve(Paths.get(fileName));

        LocalFileWriter fileWriter = createLocalFileWriter(fullPath);

        writerMap.put(partitionedPath, fileWriter);
        return fileWriter;
    }

    public LocalFileWriter createLocalFileWriter(Path fullPath) throws IOException {
        switch (writerType) {
            case PARQUET:
                return new LocalParquetFileWriter(System.currentTimeMillis(), fullPath.toString(), pageSize, blockSize, messageDescriptor, metadataFieldDescriptor);
            case MEMORY:
                return new MemoryWriter(System.currentTimeMillis());
            default:
                throw new IOException("unsupported LOCAL_FILE_WRITER_TYPE");
        }
    }

    private Boolean shouldRotate(LocalFileWriter writer) {
        return policies.stream().reduce(false,
                (accumulated, writerPolicy) -> accumulated || writerPolicy.shouldRotate(writer), (left, right) -> left || right);
    }

    @Override
    public void close() throws IOException {
        for (LocalFileWriter writer : writerMap.values()) {
            writer.close();
        }
    }
}
