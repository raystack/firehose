package io.odpf.firehose.sink.objectstorage.writer.remote;

import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ObjectStorageFileCheckerWorker implements Runnable {
    private final BlockingQueue<String> toBeFlushedToRemotePaths;
    private final BlockingQueue<String> flushedToRemotePaths;
    private final BlockingQueue<ObjectStorageWriterWorkerFuture> remoteUploadFutures;
    private final ExecutorService remoteUploadScheduler;
    private final ObjectStorageWriterConfig objectStorageWriterConfig;

    @Override
    public void run() {
        List<String> tobeFlushed = new ArrayList<>();
        toBeFlushedToRemotePaths.drainTo(tobeFlushed);
        remoteUploadFutures.addAll(tobeFlushed.stream()
                .map(path -> new ObjectStorageWriterWorkerFuture(
                        remoteUploadScheduler.submit(new ObjectStorageWriterWorker(objectStorageWriterConfig, path)),
                        path)
                ).collect(Collectors.toList()));

        Set<String> flushedPath = remoteUploadFutures.stream().map(future -> {
            if (!future.getFuture().isDone()) {
                return "";
            } else {
                try {
                    future.getFuture().get();
                } catch (InterruptedException e) {
                    throw new ObjectStorageUploadFailedException(e);
                } catch (ExecutionException e) {
                    throw new ObjectStorageUploadFailedException(e.getCause());
                }
                return future.getPath();
            }
        }).filter(x -> !x.isEmpty()).collect(Collectors.toSet());
        remoteUploadFutures.removeIf(x -> flushedPath.contains(x.getPath()));
        flushedToRemotePaths.addAll(flushedPath);
    }


}
