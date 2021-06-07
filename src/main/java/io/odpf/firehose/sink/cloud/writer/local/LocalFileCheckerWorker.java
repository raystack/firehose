package io.odpf.firehose.sink.cloud.writer.local;

import io.odpf.firehose.sink.cloud.writer.local.policy.WriterPolicy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class LocalFileCheckerWorker implements Runnable {
    private final BlockingQueue<String> toBeFlushedToCloudPathList;
    private final Map<Path, LocalFileWriter> writerMap;
    private final List<WriterPolicy> policies;

    public LocalFileCheckerWorker(BlockingQueue<String> toBeFlushedToCloudPathList,
                                  Map<Path, LocalFileWriter> writerMap,
                                  List<WriterPolicy> policies) {
        this.toBeFlushedToCloudPathList = toBeFlushedToCloudPathList;
        this.writerMap = writerMap;
        this.policies = policies;
    }

    @Override
    public void run() {
        synchronized (writerMap) {
            writerMap.entrySet().removeIf(kv -> {
                if (shouldRotate(kv.getValue())) {
                    try {
                        kv.getValue().close();
                        toBeFlushedToCloudPathList.add(kv.getValue().getFullPath());
                    } catch (IOException e) {
                        e.printStackTrace();
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
