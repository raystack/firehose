package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalStorage;
import lombok.AllArgsConstructor;
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
import java.util.stream.Collectors;

import static io.odpf.firehose.metrics.Metrics.*;

@AllArgsConstructor
public class ObjectStorageChecker implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStorageChecker.class);
    private final BlockingQueue<String> toBeFlushedToRemotePaths;
    private final BlockingQueue<String> flushedToRemotePaths;
    private final Set<ObjectStorageWriterWorkerFuture> remoteUploadFutures;
    private final ExecutorService remoteUploadScheduler;
    private final ObjectStorage objectStorage;
    private final LocalStorage localStorage;
    private final Instrumentation instrumentation;

    @Override
    public void run() {
        List<String> tobeFlushed = new ArrayList<>();
        toBeFlushedToRemotePaths.drainTo(tobeFlushed);
        remoteUploadFutures.addAll(tobeFlushed.stream()
                .map(path -> new ObjectStorageWriterWorkerFuture(
                        remoteUploadScheduler.submit(() -> {
                            objectStorage.store(path);
                        }), path, Instant.now())
                ).collect(Collectors.toList()));

        Set<String> flushedPath = remoteUploadFutures.stream().map(future -> {
            if (!future.getFuture().isDone()) {
                return "";
            } else {
                try {
                    future.getFuture().get();
                    LOGGER.info("Flushed to Object storage " + future.getPath());
                    instrumentation.incrementCounterWithTags(SINK_OBJECTSTORAGE_FILE_UPLOAD_TOTAL, SUCCESS_TAG);
                    long fileSize = localStorage.getFileSize(future.getPath());
                    instrumentation.captureCountWithTags(SINK_OBJECTSTORAGE_FILE_UPLOAD_BYTES, fileSize);
                    instrumentation.captureDurationSince(SINK_OBJECTSTORAGE_FILE_UPLOAD_TIME_MILLISECONDS, future.getStartTime());
                } catch (InterruptedException e) {
                    instrumentation.incrementCounterWithTags(SINK_OBJECTSTORAGE_FILE_UPLOAD_TOTAL, FAILURE_TAG);
                    throw new ObjectStorageFailedException(e);
                } catch (ExecutionException e) {
                    instrumentation.incrementCounterWithTags(SINK_OBJECTSTORAGE_FILE_UPLOAD_TOTAL, FAILURE_TAG);
                    throw new ObjectStorageFailedException(e.getCause());
                } catch (IOException e) {
                    instrumentation.logError("failed to get file size from : {}", future.getPath());
                    throw new ObjectStorageFailedException(e);
                }
                return future.getPath();
            }
        }).filter(x -> !x.isEmpty()).collect(Collectors.toSet());
        remoteUploadFutures.removeIf(x -> flushedPath.contains(x.getPath()));
        flushedToRemotePaths.addAll(flushedPath);
    }
}
