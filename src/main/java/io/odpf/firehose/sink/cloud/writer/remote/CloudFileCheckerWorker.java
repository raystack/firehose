package io.odpf.firehose.sink.cloud.writer.remote;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class CloudFileCheckerWorker implements Runnable {
    private final BlockingQueue<String> toBeFlushedToCloudPathList;
    private final BlockingQueue<String> flushedToCloudPathList;
    private final BlockingQueue<CloudWriterWorkerFuture> cloudUploadFutures = new LinkedBlockingQueue<>();
    private final ExecutorService cloudUploadScheduler;

    public CloudFileCheckerWorker(BlockingQueue<String> toBeFlushedToCloudPathList,
                                  BlockingQueue<String> flushedToCloudPathList,
                                  ExecutorService cloudUploadScheduler) {
        this.toBeFlushedToCloudPathList = toBeFlushedToCloudPathList;
        this.flushedToCloudPathList = flushedToCloudPathList;
        this.cloudUploadScheduler = cloudUploadScheduler;
    }

    @Override
    public void run() {
        List<String> tobeFlushed = new ArrayList<>();
        toBeFlushedToCloudPathList.drainTo(tobeFlushed);
        cloudUploadFutures.addAll(tobeFlushed.stream()
                .map(path -> new CloudWriterWorkerFuture(
                        cloudUploadScheduler.submit(new CloudWriterWorker(path)), path)).collect(Collectors.toList()));

        Set<String> flushedPath = cloudUploadFutures.stream().map(future -> {
            if (!future.getFuture().isDone()) {
                return "";
            } else {
                try {
                    future.getFuture().get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    return "";
                }
                return future.getPath();
            }
        }).filter(x -> !x.isEmpty()).collect(Collectors.toSet());
        cloudUploadFutures.removeIf(x -> flushedPath.contains(x.getPath()));
        flushedToCloudPathList.addAll(flushedPath);
    }
}
