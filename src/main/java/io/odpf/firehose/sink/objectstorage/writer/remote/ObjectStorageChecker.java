package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.Metrics;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.sink.objectstorage.writer.local.FileMeta;
import io.odpf.firehose.sink.objectstorage.writer.local.Partition;
import io.odpf.firehose.util.Clock;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static io.odpf.firehose.metrics.Metrics.*;

@AllArgsConstructor
public class ObjectStorageChecker implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStorageChecker.class);
    private final BlockingQueue<FileMeta> toBeFlushedToRemotePaths;
    private final BlockingQueue<String> flushedToRemotePaths;
    private final Set<ObjectStorageWriterWorkerFuture> remoteUploadFutures;
    private final ExecutorService remoteUploadScheduler;
    private final ObjectStorage objectStorage;
    private final Clock clock;
    private final Instrumentation instrumentation;

    @Override
    public void run() {
        List<FileMeta> tobeFlushed = new ArrayList<>();
        toBeFlushedToRemotePaths.drainTo(tobeFlushed);
        remoteUploadFutures.addAll(tobeFlushed.stream()
                .map(fileMeta -> new ObjectStorageWriterWorkerFuture(
                        remoteUploadScheduler.submit(() -> {
                            objectStorage.store(fileMeta.getFullPath());
                        }), fileMeta, clock.now())
                ).collect(Collectors.toList()));

        Set<String> flushedPath = remoteUploadFutures.stream().map(uploadJob -> {
            if (!uploadJob.getFuture().isDone()) {
                return "";
            } else {
                try {
                    uploadJob.getFuture().get();
                    FileMeta fileMeta = uploadJob.getFileMeta();
                    Partition partition = fileMeta.getPartition();

                    LOGGER.info("Flushed to Object storage " + fileMeta.getFullPath());

                    String topic = partition.getTopic();
                    String datetimeTag = partition.getDatetimePathWithoutPrefix();

                    instrumentation.captureCountWithTags(SINK_OBJECTSTORAGE_RECORD_PROCESSED_TOTAL, fileMeta.getRecordCount(),
                            tag(SCOPE_TAG, SINK_OBJECT_STORAGE_SCOPE_FILE_UPLOAD),
                            tag(TOPIC_TAG, topic),
                            tag(PARTITION_TAG, datetimeTag));
                    instrumentation.incrementCounterWithTags(Metrics.SINK_OBJECTSTORAGE_FILE_UPLOAD_TOTAL,
                            SUCCESS_TAG,
                            tag(TOPIC_TAG, topic),
                            tag(PARTITION_TAG, datetimeTag));
                    instrumentation.captureCountWithTags(SINK_OBJECTSTORAGE_FILE_UPLOAD_BYTES, fileMeta.getFileSizeBytes(),
                            tag(TOPIC_TAG, topic),
                            tag(PARTITION_TAG, datetimeTag));
                    instrumentation.captureDurationSinceWithTags(SINK_OBJECTSTORAGE_FILE_UPLOAD_TIME_MILLISECONDS, uploadJob.getStartTime(),
                            tag(TOPIC_TAG, topic),
                            tag(PARTITION_TAG, datetimeTag));
                } catch (InterruptedException | ExecutionException e) {
                    Partition partition = uploadJob.getFileMeta().getPartition();
                    String topic = partition.getTopic();
                    String datetimeTag = partition.getDatetimePathWithoutPrefix();
                    instrumentation.incrementCounterWithTags(SINK_OBJECTSTORAGE_FILE_UPLOAD_TOTAL, FAILURE_TAG,
                            tag(TOPIC_TAG, topic),
                            tag(PARTITION_TAG, datetimeTag));

                    instrumentation.captureCountWithTags(SINK_OBJECTSTORAGE_RECORD_PROCESSING_FAILED_TOTAL, uploadJob.getFileMeta().getRecordCount(),
                            tag(TOPIC_TAG, topic),
                            tag(PARTITION_TAG, datetimeTag));

                    Throwable cause;
                    if (e instanceof ExecutionException) {
                        cause = e.getCause();
                    } else {
                        cause = e;
                    }
                    throw new ObjectStorageFailedException(cause);
                }

                return uploadJob.getFileMeta().getFullPath();
            }
        }).filter(x -> !x.isEmpty()).collect(Collectors.toSet());
        remoteUploadFutures.removeIf(x -> flushedPath.contains(x.getFileMeta().getFullPath()));
        flushedToRemotePaths.addAll(flushedPath);
    }
}
