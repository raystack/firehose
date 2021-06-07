package io.odpf.firehose.sink.cloud.writer;

import io.odpf.firehose.sink.cloud.message.MessageSerializer;
import io.odpf.firehose.sink.cloud.message.Record;
import io.odpf.firehose.sink.cloud.writer.local.LocalFileCheckerWorker;
import io.odpf.firehose.sink.cloud.writer.local.LocalFileWriter;
import io.odpf.firehose.sink.cloud.writer.local.LocalFileWriterWrapper;
import io.odpf.firehose.sink.cloud.writer.local.TimePartitionPath;
import io.odpf.firehose.sink.cloud.writer.local.policy.WriterPolicy;
import io.odpf.firehose.sink.cloud.writer.remote.CloudFileCheckerWorker;
import lombok.Getter;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WriterOrchestrator implements Closeable {
    private static final int FILE_CHECKER_THREAD_INITIAL_DELAY_SECONDS = 10;
    private static final int FILE_CHECKER_THREAD_FREQUENCY_SECONDS = 5;
    private final Map<Path, LocalFileWriter> writerMap = new ConcurrentHashMap<>();
    private final Path basePath;
    @Getter
    private final MessageSerializer messageSerializer;

    private final TimePartitionPath timePartitionPath;
    private final ScheduledExecutorService localFileCheckerScheduler = Executors.newScheduledThreadPool(1);
    private final ScheduledExecutorService cloudFileCheckerScheduler = Executors.newScheduledThreadPool(1);
    private final ExecutorService cloudUploadScheduler = Executors.newFixedThreadPool(10);
    private final BlockingQueue<String> flushedToCloudPathList = new LinkedBlockingQueue<>();
    private final LocalFileWriterWrapper writerWrapper;

    public WriterOrchestrator(LocalFileWriterWrapper writerWrapper,
                              List<WriterPolicy> policies,
                              TimePartitionPath timePartitionPath,
                              Path basePath,
                              MessageSerializer messageSerializer) {

        this.timePartitionPath = timePartitionPath;
        this.basePath = basePath;
        this.messageSerializer = messageSerializer;
        this.writerWrapper = writerWrapper;

        BlockingQueue<String> toBeFlushedToCloudPathList = new LinkedBlockingQueue<>();

        localFileCheckerScheduler.scheduleAtFixedRate(
                new LocalFileCheckerWorker(
                        toBeFlushedToCloudPathList,
                        writerMap,
                        policies),
                FILE_CHECKER_THREAD_INITIAL_DELAY_SECONDS,
                FILE_CHECKER_THREAD_FREQUENCY_SECONDS,
                TimeUnit.SECONDS);

        cloudFileCheckerScheduler.scheduleWithFixedDelay(
                new CloudFileCheckerWorker(
                        toBeFlushedToCloudPathList,
                        flushedToCloudPathList,
                        cloudUploadScheduler),
                FILE_CHECKER_THREAD_INITIAL_DELAY_SECONDS,
                FILE_CHECKER_THREAD_FREQUENCY_SECONDS,
                TimeUnit.SECONDS);
    }

    /**
     * @return a copy of flushed path list
     */
    public List<String> getFlushedPaths() {
        return new ArrayList<>(flushedToCloudPathList);
    }

    public void deleteFromFlushedPath(String path) {
        flushedToCloudPathList.removeIf(x -> x.equals(path));
    }

    public String write(Record record) throws IOException {
        Path partitionedPath = timePartitionPath.create(record);
        synchronized (writerMap) {
            if (!writerMap.containsKey(partitionedPath)) {
                writerMap.put(partitionedPath, this.writerWrapper.createLocalFileWriter(basePath, partitionedPath));
            }
            writerMap.get(partitionedPath).write(record);
            return writerMap.get(partitionedPath).getFullPath();
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (writerMap) {
            for (LocalFileWriter writer : writerMap.values()) {
                writer.close();
            }
        }
        localFileCheckerScheduler.shutdown();
        cloudFileCheckerScheduler.shutdown();
        cloudUploadScheduler.shutdown();
    }

}
