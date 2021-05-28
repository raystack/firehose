package io.odpf.firehose.sink.file.writer;

import com.google.protobuf.Descriptors;
import io.odpf.firehose.sink.file.message.Record;
import io.odpf.firehose.sink.file.writer.path.TimePartitionPath;
import io.odpf.firehose.sink.file.writer.policy.WriterPolicy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class PartitioningWriter implements Closeable {

    private int pageSize;
    private int blockSize;
    private Descriptors.Descriptor messageDescriptor;
    private List<Descriptors.FieldDescriptor> metadataFieldDescriptor;

    private List<WriterPolicy> policies;
    private TimePartitionPath timePartitionPath;

    private Map<Path, LocalFileWriter> writerMap = new ConcurrentHashMap<>();

    public PartitioningWriter(int pageSize, int blockSize, Descriptors.Descriptor messageDescriptor, List<Descriptors.FieldDescriptor> metadataFieldDescriptor, List<WriterPolicy> policies, TimePartitionPath timePartitionPath) {
        this.pageSize = pageSize;
        this.blockSize = blockSize;
        this.messageDescriptor = messageDescriptor;
        this.metadataFieldDescriptor = metadataFieldDescriptor;
        this.policies = policies;
        this.timePartitionPath = timePartitionPath;
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

        LocalFileWriter fileWriter = new LocalFileWriter(System.currentTimeMillis(), fullPath.toString(), pageSize, blockSize, messageDescriptor, metadataFieldDescriptor);
        writerMap.put(partitionedPath, fileWriter);
        return fileWriter;
    }

    private Boolean shouldRotate(LocalFileWriter writer) {
        return policies.stream().reduce(false,
                (accumulated, writerPolicy) -> accumulated || writerPolicy.shouldRotate(writer)
                , (left, right) -> left || right);
    }

    @Override
    public void close() throws IOException {
        Collection<LocalFileWriter> writers = writerMap.values();
        for (LocalFileWriter writer :writers) {
            writer.close();
        }
    }
}
