package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.objectstorage.ObjectStorage;
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

@AllArgsConstructor
public class ObjectStorageChecker implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStorageChecker.class);
    private final BlockingQueue<String> toBeFlushedToRemotePaths;
    private final BlockingQueue<String> flushedToRemotePaths;
    private final Set<ObjectStorageWriterWorkerFuture> remoteUploadFutures;
    private final ExecutorService remoteUploadScheduler;
    private final ObjectStorage objectStorage;

    @Override
    public void run() {
        List<String> tobeFlushed = new ArrayList<>();
        toBeFlushedToRemotePaths.drainTo(tobeFlushed);
        remoteUploadFutures.addAll(tobeFlushed.stream()
                .map(path -> new ObjectStorageWriterWorkerFuture(
                        remoteUploadScheduler.submit(() -> objectStorage.store(path)), path)
                ).collect(Collectors.toList()));

        Set<String> flushedPath = remoteUploadFutures.stream().map(future -> {
            if (!future.getFuture().isDone()) {
                return "";
            } else {
                try {
                    future.getFuture().get();
                    LOGGER.info("Flushed to Object storage " + future.getPath());
                } catch (InterruptedException e) {
                    throw new ObjectStorageFailedException(e);
                } catch (ExecutionException e) {
                    throw new ObjectStorageFailedException(e.getCause());
                }
                return future.getPath();
            }
        }).filter(x -> !x.isEmpty()).collect(Collectors.toSet());
        remoteUploadFutures.removeIf(x -> flushedPath.contains(x.getPath()));
        flushedToRemotePaths.addAll(flushedPath);
    }
}
