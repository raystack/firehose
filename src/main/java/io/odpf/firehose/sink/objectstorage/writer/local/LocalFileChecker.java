package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import static io.odpf.firehose.metrics.Metrics.*;
import static io.odpf.firehose.sink.objectstorage.ObjectStorageMetrics.*;

public class LocalFileChecker implements Runnable {
    private final Queue<FileMeta> toBeFlushedToRemotePaths;
    private final Map<String, LocalFileWriter> timePartitionWriterMap;
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileChecker.class);
    private final LocalStorage localStorage;
    private final Clock clock;
    private final Instrumentation instrumentation;


    public LocalFileChecker(Queue<FileMeta> toBeFlushedToRemotePaths,
                            Map<String, LocalFileWriter> timePartitionWriterMap,
                            LocalStorage localStorage,
                            Clock clock, Instrumentation instrumentation) {
        this.toBeFlushedToRemotePaths = toBeFlushedToRemotePaths;
        this.timePartitionWriterMap = timePartitionWriterMap;
        this.localStorage = localStorage;
        this.clock = clock;
        this.instrumentation = instrumentation;
    }

    @Override
    public void run() {
        Map<String, LocalFileWriter> toBeRotated;
        synchronized (timePartitionWriterMap) {
            toBeRotated = timePartitionWriterMap.entrySet().stream().filter(kv -> localStorage.shouldRotate(kv.getValue()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            timePartitionWriterMap.entrySet().removeIf(kv -> toBeRotated.containsKey(kv.getKey()));
        }
        toBeRotated.forEach((key, writer) -> {
            String filePath = writer.getFullPath();
            Partition partition = localStorage.getPartitionFactory().fromPartitionPath(key);

            try {
                Instant fileClosingStartTime = clock.now();
                writer.close();
                LOGGER.info("Closing Local File " + filePath);
                //getting size of file from file instead of writer, because there is possibility of data compression on the writer
                long fileSize = localStorage.getFileSize(filePath);
                FileMeta fileMeta = new FileMeta(filePath, writer.getRecordCount(), fileSize, partition);
                toBeFlushedToRemotePaths.add(fileMeta);
                captureFileClosedSuccessMetric(fileClosingStartTime, fileMeta);
            } catch (IOException e) {
                e.printStackTrace();
                captureFileCloseFailedMetric(partition);
                throw new LocalFileWriterFailedException(e);
            }
        });
        instrumentation.captureValue(LOCAL_FILE_OPEN_TOTAL, timePartitionWriterMap.size());
    }

    private void captureFileClosedSuccessMetric(Instant fileClosingStartTime, FileMeta fileMeta) {
        String dateTimeTag = fileMeta.getPartition().getDatetimePathWithoutPrefix();
        String topic = fileMeta.getPartition().getTopic();
        instrumentation.incrementCounterWithTags(LOCAL_FILE_CLOSE_TOTAL,
                SUCCESS_TAG,
                tag(TOPIC_TAG, topic),
                tag(PARTITION_TAG, dateTimeTag));

        instrumentation.captureDurationSinceWithTags(LOCAL_FILE_CLOSING_TIME_MILLISECONDS, fileClosingStartTime,
                tag(TOPIC_TAG, topic),
                tag(PARTITION_TAG, dateTimeTag));

        instrumentation.captureCountWithTags(LOCAL_FILE_SIZE_BYTES, fileMeta.getFileSizeBytes(),
                tag(TOPIC_TAG, topic),
                tag(PARTITION_TAG, dateTimeTag));
    }

    private void captureFileCloseFailedMetric(Partition partition) {
        String dateTimeTag = partition.getDatetimePathWithoutPrefix();
        String topic = partition.getTopic();
        instrumentation.incrementCounterWithTags(LOCAL_FILE_CLOSE_TOTAL,
                FAILURE_TAG,
                tag(TOPIC_TAG, topic),
                tag(PARTITION_TAG, dateTimeTag));
    }
}
