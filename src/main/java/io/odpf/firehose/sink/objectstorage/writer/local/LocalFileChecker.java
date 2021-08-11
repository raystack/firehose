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
            String dateTimeTag = partition.getDatetimePathWithoutPrefix();
            String topic = partition.getTopic();
            try {
                Instant fileClosingStartTime = clock.now();
                writer.close();

                //getting size of file from file instead of writer, because there is possibility of data compression on the writer
                long fileSize = localStorage.getFileSize(filePath);
                FileMeta fileMeta = new FileMeta(filePath, writer.getRecordCount(), fileSize, partition);

                toBeFlushedToRemotePaths.add(fileMeta);
                LOGGER.info("Closing Local File " + filePath);

                instrumentation.incrementCounterWithTags(SINK_OBJECTSTORAGE_LOCAL_FILE_CLOSE_TOTAL,
                        SUCCESS_TAG,
                        tag(TOPIC_TAG, topic),
                        tag(PARTITION_TAG, dateTimeTag));

                instrumentation.captureDurationSinceWithTags(SINK_OBJECTSTORAGE_LOCAL_FILE_CLOSING_TIME_MILLISECONDS, fileClosingStartTime,
                        tag(TOPIC_TAG, topic),
                        tag(PARTITION_TAG, dateTimeTag));

                instrumentation.captureCountWithTags(SINK_OBJECTSTORAGE_RECORD_PROCESSED_TOTAL, fileMeta.getRecordCount(),
                        tag(SCOPE_TAG, SINK_OBJECT_STORAGE_SCOPE_FILE_CLOSE),
                        tag(TOPIC_TAG, topic),
                        tag(PARTITION_TAG, dateTimeTag));

                instrumentation.captureCountWithTags(SINK_OBJECTSTORAGE_LOCAL_FILE_SIZE_BYTES, fileMeta.getFileSizeBytes(),
                        tag(TOPIC_TAG, topic),
                        tag(PARTITION_TAG, dateTimeTag));
            } catch (IOException e) {
                e.printStackTrace();
                instrumentation.captureCountWithTags(SINK_OBJECTSTORAGE_RECORD_PROCESSING_FAILED_TOTAL, writer.getRecordCount(),
                        tag(TOPIC_TAG, topic),
                        tag(PARTITION_TAG, dateTimeTag));
                instrumentation.incrementCounterWithTags(SINK_OBJECTSTORAGE_LOCAL_FILE_CLOSE_TOTAL,
                        FAILURE_TAG,
                        tag(TOPIC_TAG, topic),
                        tag(PARTITION_TAG, dateTimeTag));
                throw new LocalFileWriterFailedException(e);
            }
        });
    }
}
