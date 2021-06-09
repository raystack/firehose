package io.odpf.firehose.sink.objectstorage.writer.remote;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class ObjectStorageFileCheckerWorker implements Runnable {
    private final BlockingQueue<String> toBeFlushedToRemotePaths;
    private final BlockingQueue<String> flushedToRemotePaths;
    private final BlockingQueue<ObjectStorageWriterWorkerFuture> remoteUploadFutures;
    private final ExecutorService remoteUploadScheduler;

    private final String projectID;
    private final String bucketName;
    private final String basePath;

    public ObjectStorageFileCheckerWorker(BlockingQueue<String> toBeFlushedToRemotePaths,
                                          BlockingQueue<String> flushedToRemotePaths,
                                          BlockingQueue<ObjectStorageWriterWorkerFuture> remoteUploadFutures,
                                          ExecutorService remoteUploadScheduler,
                                          String projectID,
                                          String bucketName,
                                          String basePath) {
        this.toBeFlushedToRemotePaths = toBeFlushedToRemotePaths;
        this.flushedToRemotePaths = flushedToRemotePaths;
        this.remoteUploadFutures = remoteUploadFutures;
        this.remoteUploadScheduler = remoteUploadScheduler;
        this.projectID = projectID;
        this.bucketName = bucketName;
        this.basePath = basePath;
    }

    @Override
    public void run() {
        List<String> tobeFlushed = new ArrayList<>();
        toBeFlushedToRemotePaths.drainTo(tobeFlushed);
        remoteUploadFutures.addAll(tobeFlushed.stream()
                .map(path -> new ObjectStorageWriterWorkerFuture(
                        remoteUploadScheduler.submit(new ObjectStorageWriterWorker(projectID, bucketName, basePath, path)), path)).collect(Collectors.toList()));

        Set<String> flushedPath = remoteUploadFutures.stream().map(future -> {
            if (!future.getFuture().isDone()) {
                return "";
            } else {
                try {
                    future.getFuture().get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    throw new ObjectStorageUploadFailedException(e);
                }
                return future.getPath();
            }
        }).filter(x -> !x.isEmpty()).collect(Collectors.toSet());
        remoteUploadFutures.removeIf(x -> flushedPath.contains(x.getPath()));
        flushedToRemotePaths.addAll(flushedPath);
    }


}
