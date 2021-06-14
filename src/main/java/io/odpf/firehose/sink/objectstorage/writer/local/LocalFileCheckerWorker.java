package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.sink.objectstorage.writer.local.policy.WriterPolicy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class LocalFileCheckerWorker implements Runnable {
    private final Queue<String> toBeFlushedToRemotePaths;
    private final Map<String, LocalFileWriter> timePartitionWriterMap;
    private final List<WriterPolicy> policies;

    public LocalFileCheckerWorker(Queue<String> toBeFlushedToRemotePaths,
                                  Map<String, LocalFileWriter> timePartitionWriterMap,
                                  List<WriterPolicy> policies) {
        this.toBeFlushedToRemotePaths = toBeFlushedToRemotePaths;
        this.timePartitionWriterMap = timePartitionWriterMap;
        this.policies = policies;
    }

    @Override
    public void run() {
        synchronized (timePartitionWriterMap) {
            timePartitionWriterMap.entrySet().removeIf(kv -> {
                if (shouldRotate(kv.getValue())) {
                    try {
                        kv.getValue().close();
                        toBeFlushedToRemotePaths.add(kv.getValue().getFullPath());
                        System.out.println("Adding file to be flushed " + kv.getValue().getFullPath());
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new LocalFileWriterFailedException(e);
                    }
                    return true;
                }
                return false;
            });
        }
    }

    private Boolean shouldRotate(LocalFileWriter writer) {
        return policies.stream().reduce(false,
                (accumulated, writerPolicy) -> accumulated || writerPolicy.shouldRotate(writer), (left, right) -> left || right);
    }
}
