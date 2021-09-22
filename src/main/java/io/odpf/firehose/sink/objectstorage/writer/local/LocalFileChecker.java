package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.metrics.Instrumentation;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import static io.odpf.firehose.metrics.Metrics.FAILURE_TAG;
import static io.odpf.firehose.metrics.Metrics.SUCCESS_TAG;
import static io.odpf.firehose.metrics.Metrics.tag;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.LOCAL_FILE_CLOSE_TOTAL;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.LOCAL_FILE_CLOSING_TIME_MILLISECONDS;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.LOCAL_FILE_OPEN_TOTAL;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.LOCAL_FILE_SIZE_BYTES;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.TOPIC_TAG;

public class LocalFileChecker implements Runnable {
    private final Queue<FileMeta> toBeFlushedToRemotePaths;
    private final Map<String, LocalFileWriter> timePartitionWriterMap;
    private final LocalStorage localStorage;
    private final Instrumentation instrumentation;


    public LocalFileChecker(Queue<FileMeta> toBeFlushedToRemotePaths,
                            Map<String, LocalFileWriter> timePartitionWriterMap,
                            LocalStorage localStorage,
                            Instrumentation instrumentation) {
        this.toBeFlushedToRemotePaths = toBeFlushedToRemotePaths;
        this.timePartitionWriterMap = timePartitionWriterMap;
        this.localStorage = localStorage;
        this.instrumentation = instrumentation;
    }

    @Override
    public void run() {
        instrumentation.captureValue(LOCAL_FILE_OPEN_TOTAL, timePartitionWriterMap.size());
        Map<String, LocalFileWriter> toBeRotated;
        toBeRotated = timePartitionWriterMap.entrySet().stream().filter(kv -> localStorage.shouldRotate(kv.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        timePartitionWriterMap.entrySet().removeIf(kv -> toBeRotated.containsKey(kv.getKey()));
        toBeRotated.forEach((path, writer) -> {
            String filePath = writer.getFullPath();
            FilePartitionPath filePartitionPath = localStorage.getFilePartitionPathFactory().fromFilePartitionPath(path);
            try {
                Instant startTime = Instant.now();
                writer.close();
                instrumentation.logInfo("Closing Local File {} ", filePath);
                long fileSize = localStorage.getFileSize(filePath);
                FileMeta fileMeta = new FileMeta(filePath, writer.getRecordCount(), fileSize, filePartitionPath);
                toBeFlushedToRemotePaths.add(fileMeta);
                captureFileClosedSuccessMetric(startTime, fileMeta);
            } catch (IOException e) {
                e.printStackTrace();
                captureFileCloseFailedMetric(filePartitionPath);
                throw new LocalFileWriterFailedException(e);
            }
        });
        instrumentation.captureValue(LOCAL_FILE_OPEN_TOTAL, timePartitionWriterMap.size());
    }

    private void captureFileClosedSuccessMetric(Instant startTime, FileMeta fileMeta) {
        String topic = fileMeta.getFilePartitionPath().getTopic();
        instrumentation.incrementCounter(LOCAL_FILE_CLOSE_TOTAL,
                SUCCESS_TAG,
                tag(TOPIC_TAG, topic));

        instrumentation.captureDurationSince(LOCAL_FILE_CLOSING_TIME_MILLISECONDS, startTime,
                tag(TOPIC_TAG, topic));

        instrumentation.captureCount(LOCAL_FILE_SIZE_BYTES, fileMeta.getFileSizeBytes(),
                tag(TOPIC_TAG, topic));
    }

    private void captureFileCloseFailedMetric(FilePartitionPath filePartitionPath) {
        String topic = filePartitionPath.getTopic();
        instrumentation.incrementCounter(LOCAL_FILE_CLOSE_TOTAL,
                FAILURE_TAG,
                tag(TOPIC_TAG, topic));
    }
}
