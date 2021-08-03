package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.metrics.Instrumentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import static io.odpf.firehose.metrics.Metrics.*;

public class LocalFileChecker implements Runnable {
    private final Queue<String> toBeFlushedToRemotePaths;
    private final Map<String, LocalFileWriter> timePartitionWriterMap;
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileChecker.class);
    private final Instrumentation instrumentation;
    private final LocalStorage localStorage;


    public LocalFileChecker(Queue<String> toBeFlushedToRemotePaths,
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
        Map<String, LocalFileWriter> toBeRotated;
        synchronized (timePartitionWriterMap) {
            toBeRotated = timePartitionWriterMap.entrySet().stream().filter(kv -> localStorage.shouldRotate(kv.getValue()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            timePartitionWriterMap.entrySet().removeIf(kv -> toBeRotated.containsKey(kv.getKey()));
        }
        toBeRotated.values().forEach(
                writer -> {
                    try {
                        Instant fileClosingStartTime = Instant.now();
                        writer.close();
                        String filePath = writer.getFullPath();
                        LOGGER.info("Closing Local File " + filePath);

                        instrumentation.captureDurationSince(SINK_OBJECTSTORAGE_LOCAL_FILE_CLOSING_TIME_MILLISECONDS, fileClosingStartTime);

                        toBeFlushedToRemotePaths.add(filePath);
                        instrumentation.incrementCounterWithTags(SINK_OBJECTSTORAGE_LOCAL_FILE_CLOSED_TOTAL, SUCCESS_TAG);

                        long fileSize = localStorage.getFileSize(filePath);
                        instrumentation.captureCountWithTags(SINK_OBJECTSTORAGE_LOCAL_FILE_SIZE_BYTES, fileSize);
                    } catch (IOException e) {
                        e.printStackTrace();
                        instrumentation.incrementCounterWithTags(SINK_OBJECTSTORAGE_LOCAL_FILE_CLOSED_TOTAL, FAILURE_TAG);
                        throw new LocalFileWriterFailedException(e);
                    }
                });
    }
}
