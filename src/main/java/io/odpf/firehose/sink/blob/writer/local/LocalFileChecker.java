package io.odpf.firehose.sink.blob.writer.local;

import io.odpf.firehose.metrics.FirehoseInstrumentation;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import static io.odpf.firehose.metrics.Metrics.FAILURE_TAG;
import static io.odpf.firehose.metrics.Metrics.SUCCESS_TAG;
import static io.odpf.firehose.metrics.BlobStorageMetrics.*;

public class LocalFileChecker implements Runnable {
    private final Queue<LocalFileMetadata> toBeFlushedToRemotePaths;
    private final Map<Path, LocalFileWriter> timePartitionWriterMap;
    private final LocalStorage localStorage;
    private final FirehoseInstrumentation firehoseInstrumentation;


    public LocalFileChecker(Queue<LocalFileMetadata> toBeFlushedToRemotePaths,
                            Map<Path, LocalFileWriter> timePartitionWriterMap,
                            LocalStorage localStorage,
                            FirehoseInstrumentation firehoseInstrumentation) {
        this.toBeFlushedToRemotePaths = toBeFlushedToRemotePaths;
        this.timePartitionWriterMap = timePartitionWriterMap;
        this.localStorage = localStorage;
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    @Override
    public void run() {
        firehoseInstrumentation.captureValue(LOCAL_FILE_OPEN_TOTAL, timePartitionWriterMap.size());
        Map<Path, LocalFileWriter> toBeRotated =
                timePartitionWriterMap.entrySet().stream().filter(kv -> localStorage.shouldRotate(kv.getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        timePartitionWriterMap.entrySet().removeAll(toBeRotated.entrySet());
        toBeRotated.forEach((path, writer) -> {
            try {
                Instant startTime = Instant.now();
                LocalFileMetadata metadata = writer.closeAndFetchMetaData();
                firehoseInstrumentation.logInfo("Closing Local File {} ", metadata.getFullPath());
                toBeFlushedToRemotePaths.add(metadata);
                captureFileClosedSuccessMetric(startTime, metadata);
            } catch (IOException e) {
                e.printStackTrace();
                captureFileCloseFailedMetric();
                throw new LocalFileWriterFailedException(e);
            }
        });
        firehoseInstrumentation.captureValue(LOCAL_FILE_OPEN_TOTAL, timePartitionWriterMap.size());
    }

    private void captureFileClosedSuccessMetric(Instant startTime, LocalFileMetadata localFileMetadata) {
        firehoseInstrumentation.incrementCounter(LOCAL_FILE_CLOSE_TOTAL, SUCCESS_TAG);
        firehoseInstrumentation.captureDurationSince(LOCAL_FILE_CLOSING_TIME_MILLISECONDS, startTime);
        firehoseInstrumentation.captureCount(LOCAL_FILE_SIZE_BYTES, localFileMetadata.getSize());
        firehoseInstrumentation.captureCount(LOCAL_FILE_RECORDS_TOTAL, localFileMetadata.getRecordCount());
    }

    private void captureFileCloseFailedMetric() {
        firehoseInstrumentation.incrementCounter(LOCAL_FILE_CLOSE_TOTAL, FAILURE_TAG);
    }
}
