package io.odpf.firehose.sink.objectstorage.writer;

import io.odpf.firehose.sink.objectstorage.message.MessageSerializer;
import io.odpf.firehose.sink.objectstorage.message.Record;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileCheckerWorker;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileWriter;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileWriterWrapper;
import io.odpf.firehose.sink.objectstorage.writer.local.TimePartitionPath;
import io.odpf.firehose.sink.objectstorage.writer.local.policy.WriterPolicy;
import io.odpf.firehose.sink.objectstorage.writer.remote.ObjectStorageFileCheckerWorker;
import io.odpf.firehose.sink.objectstorage.writer.remote.ObjectStorageWriterWorkerFuture;
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
    private final Map<String, LocalFileWriter> timePartitionWriterMap = new ConcurrentHashMap<>();
    private final Path basePath;
    @Getter
    private final MessageSerializer messageSerializer;

    private final TimePartitionPath timePartitionPath;
    private final ScheduledExecutorService localFileCheckerScheduler = Executors.newScheduledThreadPool(1);
    private final ScheduledExecutorService remoteFileCheckerScheduler = Executors.newScheduledThreadPool(1);
    private final ExecutorService remoteUploadScheduler = Executors.newFixedThreadPool(10);
    private final BlockingQueue<String> flushedToRemotePaths = new LinkedBlockingQueue<>();
    private final LocalFileWriterWrapper writerWrapper;

    private String projectID;
    private String bucketName;

    public WriterOrchestrator(LocalFileWriterWrapper writerWrapper,
                              List<WriterPolicy> policies,
                              TimePartitionPath timePartitionPath,
                              Path basePath,
                              MessageSerializer messageSerializer,
                              String projectID,
                              String bucketName) {

        this.timePartitionPath = timePartitionPath;
        this.basePath = basePath;
        this.messageSerializer = messageSerializer;
        this.writerWrapper = writerWrapper;

        BlockingQueue<String> toBeFlushedToRemotePaths = new LinkedBlockingQueue<>();
        BlockingQueue<ObjectStorageWriterWorkerFuture> remoteUploadFutures = new LinkedBlockingQueue<>();

        localFileCheckerScheduler.scheduleAtFixedRate(
                new LocalFileCheckerWorker(
                        toBeFlushedToRemotePaths,
                        timePartitionWriterMap,
                        policies),
                FILE_CHECKER_THREAD_INITIAL_DELAY_SECONDS,
                FILE_CHECKER_THREAD_FREQUENCY_SECONDS,
                TimeUnit.SECONDS);

        remoteFileCheckerScheduler.scheduleWithFixedDelay(
                new ObjectStorageFileCheckerWorker(
                        toBeFlushedToRemotePaths,
                        flushedToRemotePaths,
                        remoteUploadFutures,
                        remoteUploadScheduler, projectID, bucketName, basePath.toString()),
                FILE_CHECKER_THREAD_INITIAL_DELAY_SECONDS,
                FILE_CHECKER_THREAD_FREQUENCY_SECONDS,
                TimeUnit.SECONDS);
    }

    /**
     * @return a copy of flushed path list
     */
    public List<String> getFlushedPaths() {
        return new ArrayList<>(flushedToRemotePaths);
    }

    public void deleteFromFlushedPath(String path) {
        flushedToRemotePaths.removeIf(x -> x.equals(path));
    }

    public String write(Record record) throws IOException {
        Path partitionedPath = timePartitionPath.create(record);
        synchronized (timePartitionWriterMap) {
            if (!timePartitionWriterMap.containsKey(partitionedPath.toString())) {
                timePartitionWriterMap.put(partitionedPath.toString(), this.writerWrapper.createLocalFileWriter(basePath, partitionedPath));
            }
            timePartitionWriterMap.get(partitionedPath.toString()).write(record);
            return timePartitionWriterMap.get(partitionedPath.toString()).getFullPath();
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (timePartitionWriterMap) {
            for (LocalFileWriter writer : timePartitionWriterMap.values()) {
                writer.close();
            }
        }
        localFileCheckerScheduler.shutdown();
        remoteFileCheckerScheduler.shutdown();
        remoteUploadScheduler.shutdown();
    }

}
