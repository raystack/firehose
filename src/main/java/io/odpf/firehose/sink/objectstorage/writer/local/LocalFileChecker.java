package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.metrics.Instrumentation;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import static io.odpf.firehose.metrics.Metrics.FAILURE_TAG;
import static io.odpf.firehose.metrics.Metrics.SUCCESS_TAG;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.*;

public class LocalFileChecker implements Runnable {
    private final Queue<LocalFileMetadata> toBeFlushedToRemotePaths;
    private final Map<Path, LocalFileWriter> timePartitionWriterMap;
    private final LocalStorage localStorage;
    private final Instrumentation instrumentation;


    public LocalFileChecker(Queue<LocalFileMetadata> toBeFlushedToRemotePaths,
                            Map<Path, LocalFileWriter> timePartitionWriterMap,
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
        Map<Path, LocalFileWriter> toBeRotated;
        toBeRotated = timePartitionWriterMap.entrySet().stream().filter(kv -> localStorage.shouldRotate(kv.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        timePartitionWriterMap.entrySet().removeAll(toBeRotated.entrySet());
        toBeRotated.forEach((path, writer) -> {
            try {
                LocalFileMetadata metadata = writer.getMetadata();
                Instant startTime = Instant.now();
                writer.close();
                instrumentation.logInfo("Closing Local File {} ", metadata.getFullPath());
                toBeFlushedToRemotePaths.add(metadata);
                captureFileClosedSuccessMetric(startTime, metadata);
            } catch (IOException e) {
                e.printStackTrace();
                captureFileCloseFailedMetric();
                throw new LocalFileWriterFailedException(e);
            }
        });
        instrumentation.captureValue(LOCAL_FILE_OPEN_TOTAL, timePartitionWriterMap.size());
    }

    private void captureFileClosedSuccessMetric(Instant startTime, LocalFileMetadata localFileMetadata) {
        instrumentation.incrementCounter(LOCAL_FILE_CLOSE_TOTAL, SUCCESS_TAG);
        instrumentation.captureDurationSince(LOCAL_FILE_CLOSING_TIME_MILLISECONDS, startTime);
        instrumentation.captureCount(LOCAL_FILE_SIZE_BYTES, localFileMetadata.getSize());
        instrumentation.captureCount(LOCAL_FILE_RECORDS_TOTAL, localFileMetadata.getRecordCount());
    }

    private void captureFileCloseFailedMetric() {
        instrumentation.incrementCounter(LOCAL_FILE_CLOSE_TOTAL, FAILURE_TAG);
    }
}
