package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileMetadata;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ObjectStorageChecker implements Runnable {

    private final BlockingQueue<LocalFileMetadata> toBeFlushedToRemotePaths;
    private final BlockingQueue<String> flushedToRemotePaths;
    private final Set<ObjectStorageWriterFutureHandler> remoteUploadFutures;
    private final ExecutorService remoteUploadScheduler;
    private final ObjectStorage objectStorage;
    private final Instrumentation instrumentation;

    @Override
    public void run() {
        List<LocalFileMetadata> tobeFlushed = new ArrayList<>();
        toBeFlushedToRemotePaths.drainTo(tobeFlushed);
        remoteUploadFutures.addAll(tobeFlushed.stream().map(this::submitTask).collect(Collectors.toList()));
        Set<ObjectStorageWriterFutureHandler> flushed = remoteUploadFutures.stream().filter(ObjectStorageWriterFutureHandler::isFinished).collect(Collectors.toSet());
        remoteUploadFutures.removeAll(flushed);
        flushedToRemotePaths.addAll(flushed.stream().map(ObjectStorageWriterFutureHandler::getFullPath).collect(Collectors.toSet()));
    }

    private ObjectStorageWriterFutureHandler submitTask(LocalFileMetadata localFileMetadata) {
        ObjectStorageWorker worker = new ObjectStorageWorker(objectStorage, localFileMetadata);
        Future<Long> f = remoteUploadScheduler.submit(worker);
        return new ObjectStorageWriterFutureHandler(f, localFileMetadata, instrumentation);
    }
}

