package io.odpf.firehose.sink.objectstorage.writer.remote;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class ObjectStorageFileCheckerWorker implements Runnable {
    private final BlockingQueue<String> toBeFlushedToRemotePaths;
    private final BlockingQueue<String> flushedToRemotePaths;
    private final BlockingQueue<ObjectStorageWriterWorkerFuture> remoteUploadFutures = new LinkedBlockingQueue<>();
    private final ExecutorService remoteUploadScheduler;

    public ObjectStorageFileCheckerWorker(BlockingQueue<String> toBeFlushedToRemotePaths,
                                          BlockingQueue<String> flushedToRemotePaths,
                                          ExecutorService remoteUploadScheduler) {
        this.toBeFlushedToRemotePaths = toBeFlushedToRemotePaths;
        this.flushedToRemotePaths = flushedToRemotePaths;
        this.remoteUploadScheduler = remoteUploadScheduler;
    }

    @Override
    public void run() {
        List<String> tobeFlushed = new ArrayList<>();
        toBeFlushedToRemotePaths.drainTo(tobeFlushed);
        remoteUploadFutures.addAll(tobeFlushed.stream()
                .map(path -> new ObjectStorageWriterWorkerFuture(
                        remoteUploadScheduler.submit(new ObjectStorageWriterWorker(path)), path)).collect(Collectors.toList()));

        Set<String> flushedPath = remoteUploadFutures.stream().map(future -> {
            if (!future.getFuture().isDone()) {
                return "";
            } else {
                try {
                    future.getFuture().get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                return future.getPath();
            }
        }).filter(x -> !x.isEmpty()).collect(Collectors.toSet());
        remoteUploadFutures.removeIf(x -> flushedPath.contains(x.getPath()));
        flushedToRemotePaths.addAll(flushedPath);
    }

    @AllArgsConstructor
    @Data
    static class ObjectStorageWriterWorkerFuture {
        private Future future;
        private String path;
    }
}
