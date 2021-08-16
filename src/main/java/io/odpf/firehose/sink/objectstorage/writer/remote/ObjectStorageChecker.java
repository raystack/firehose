package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.objectstorage.ObjectStorageException;
import io.odpf.firehose.objectstorage.gcs.exception.GCSException;
import io.odpf.firehose.sink.objectstorage.writer.local.FileMeta;
import io.odpf.firehose.sink.objectstorage.writer.local.Partition;
import io.odpf.firehose.util.Clock;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static io.odpf.firehose.metrics.Metrics.*;
import static io.odpf.firehose.sink.objectstorage.ObjectStorageMetrics.*;

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
                .map(fileMeta -> {
                            Runnable uploadTask = createUploadTask(fileMeta);
                            Future<?> future = remoteUploadScheduler.submit(uploadTask);
                            Instant startTime = clock.now();
                            return new ObjectStorageWriterWorkerFuture(future, fileMeta, startTime);
                        }
                ).collect(Collectors.toList()));

        Set<String> flushedPath = remoteUploadFutures.stream().map(uploadJob -> {
            if (!uploadJob.getFuture().isDone()) {
                return "";
            } else {
                try {
                    uploadJob.getFuture().get();
                    FileMeta fileMeta = uploadJob.getFileMeta();
                    LOGGER.info("Flushed to Object storage " + fileMeta.getFullPath());
                    captureFileUploadSuccessMetric(uploadJob, fileMeta);
                } catch (InterruptedException | ExecutionException e) {
                    captureUploadFailedMetric(uploadJob, e);
                    throw new RuntimeException(e);
                }

                return uploadJob.getFileMeta().getFullPath();
            }
        }).filter(x -> !x.isEmpty()).collect(Collectors.toSet());
        remoteUploadFutures.removeIf(x -> flushedPath.contains(x.getFileMeta().getFullPath()));
        flushedToRemotePaths.addAll(flushedPath);
    }

    private Runnable createUploadTask(FileMeta fileMeta) {
        return () -> {
            try {
                objectStorage.store(fileMeta.getFullPath());
            } catch (ObjectStorageException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void captureFileUploadSuccessMetric(ObjectStorageWriterWorkerFuture uploadJob, FileMeta fileMeta) {
        Partition partition = fileMeta.getPartition();
        String topic = partition.getTopic();
        String datetimeTag = partition.getDatetimePathWithoutPrefix();

        instrumentation.incrementCounterWithTags(FILE_UPLOAD_TOTAL,
                SUCCESS_TAG,
                tag(TOPIC_TAG, topic),
                tag(PARTITION_TAG, datetimeTag));
        instrumentation.captureCountWithTags(FILE_UPLOAD_BYTES, fileMeta.getFileSizeBytes(),
                tag(TOPIC_TAG, topic),
                tag(PARTITION_TAG, datetimeTag));
        instrumentation.captureDurationSinceWithTags(FILE_UPLOAD_TIME_MILLISECONDS, uploadJob.getStartTime(),
                tag(TOPIC_TAG, topic),
                tag(PARTITION_TAG, datetimeTag));
    }

    private void captureUploadFailedMetric(ObjectStorageWriterWorkerFuture uploadJob, Exception e) {
        Partition partition = uploadJob.getFileMeta().getPartition();
        String topic = partition.getTopic();
        String datetimeTag = partition.getDatetimePathWithoutPrefix();
        String errorType = getErrorType(e);

        instrumentation.incrementCounterWithTags(FILE_UPLOAD_TOTAL,
                FAILURE_TAG,
                tag(ERROR_TYPE_TAG, errorType),
                tag(TOPIC_TAG, topic),
                tag(PARTITION_TAG, datetimeTag));
    }

    private String getErrorType(Throwable throwable) {
        if (io.odpf.firehose.util.ExceptionUtils.matchCause(throwable, InterruptedException.class)) {
            return Constants.OBJECT_STORAGE_CHECKER_THREAD_ERROR;
        } else if (io.odpf.firehose.util.ExceptionUtils.matchCause(throwable, IOException.class)) {
            return Constants.FILE_IO_ERROR;
        } else if (ExceptionUtils.getRootCause(throwable) instanceof GCSException) {
            GCSException gcsException = (GCSException) ExceptionUtils.getRootCause(throwable);
            if (gcsException.getErrorType() != null) {
                return gcsException.getErrorType().name();
            } else {
                return String.valueOf(gcsException.getErrorCode());
            }
        }
        return "";
    }
}

