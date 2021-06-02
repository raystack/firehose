package io.odpf.firehose.sink.cloud.writer;

import com.google.protobuf.Descriptors;
import io.odpf.firehose.sink.cloud.Constants;
import io.odpf.firehose.sink.cloud.message.MessageSerializer;
import io.odpf.firehose.sink.cloud.message.Record;
import io.odpf.firehose.sink.cloud.writer.path.TimePartitionPath;
import io.odpf.firehose.sink.cloud.writer.policy.WriterPolicy;
import lombok.Getter;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WriterOrchestrator implements Closeable {
    private static final int FILE_CHECKER_THREAD_INITIAL_DELAY_SECONDS = 10;
    private static final int FILE_CHECKER_THREAD_FREQUENCY_SECONDS = 5;
    private final Map<Path, LocalFileWriter> writerMap = new ConcurrentHashMap<>();
    private final Constants.LocalFileWriterType writerType;
    private final int pageSize;
    private final int blockSize;
    private final Path basePath;
    @Getter
    private final MessageSerializer messageSerializer;
    private final Descriptors.Descriptor messageDescriptor;
    private final List<Descriptors.FieldDescriptor> metadataFieldDescriptor;

    private final List<WriterPolicy> policies;
    private final TimePartitionPath timePartitionPath;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public WriterOrchestrator(int pageSize,
                              int blockSize,
                              Descriptors.Descriptor messageDescriptor,
                              List<Descriptors.FieldDescriptor> metadataFieldDescriptor,
                              List<WriterPolicy> policies,
                              TimePartitionPath timePartitionPath,
                              Constants.LocalFileWriterType writerType,
                              Path basePath,
                              MessageSerializer messageSerializer) {

        this.pageSize = pageSize;
        this.blockSize = blockSize;
        this.messageDescriptor = messageDescriptor;
        this.metadataFieldDescriptor = metadataFieldDescriptor;
        this.policies = policies;
        this.timePartitionPath = timePartitionPath;
        this.writerType = writerType;
        this.basePath = basePath;
        this.messageSerializer = messageSerializer;
        scheduler.scheduleAtFixedRate(() -> {
            synchronized (writerMap) {
                writerMap.entrySet().removeIf(kv -> {
                    if (shouldRotate(kv.getValue())) {
                        try {
                            kv.getValue().close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return true;
                    }
                    return false;
                });
            }
        }, FILE_CHECKER_THREAD_INITIAL_DELAY_SECONDS, FILE_CHECKER_THREAD_FREQUENCY_SECONDS, TimeUnit.SECONDS);
    }

    public LocalFileWriter getWriter(Record record) throws IOException {
        Path partitionedPath = timePartitionPath.create(record);
        synchronized (writerMap) {
            if (writerMap.containsKey(partitionedPath)) {
                if (!shouldRotate(writerMap.get(partitionedPath))) {
                    return writerMap.get(partitionedPath);
                } else {
                    try {
                        writerMap.get(partitionedPath).close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            String fileName = UUID.randomUUID().toString();
            Path dir = basePath.resolve(partitionedPath);
            Path fullPath = dir.resolve(Paths.get(fileName));
            writerMap.put(partitionedPath, createLocalFileWriter(fullPath));
            return writerMap.get(partitionedPath);
        }
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
        synchronized (writerMap) {
            for (LocalFileWriter writer : writerMap.values()) {
                writer.close();
            }
        }
        scheduler.shutdown();
    }


}
