package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.sink.objectstorage.writer.local.policy.WriterPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

public class LocalFileChecker implements Runnable {
    private final Queue<String> toBeFlushedToRemotePaths;
    private final Map<String, LocalFileWriter> timePartitionWriterMap;
    private final List<WriterPolicy> policies;
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileChecker.class);

    public LocalFileChecker(Queue<String> toBeFlushedToRemotePaths,
                            Map<String, LocalFileWriter> timePartitionWriterMap,
                            List<WriterPolicy> policies) {
        this.toBeFlushedToRemotePaths = toBeFlushedToRemotePaths;
        this.timePartitionWriterMap = timePartitionWriterMap;
        this.policies = policies;
    }

    @Override
    public void run() {
        Map<String, LocalFileWriter> toBeRotated;
        synchronized (timePartitionWriterMap) {
            toBeRotated = timePartitionWriterMap.entrySet().stream().filter(kv -> shouldRotate(kv.getValue()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            timePartitionWriterMap.entrySet().removeIf(kv -> toBeRotated.containsKey(kv.getKey()));
        }
        toBeRotated.values().forEach(
                writer -> {
                    try {
                        writer.close();
                        LOGGER.info("Closing Local File " + writer.getFullPath());
                        toBeFlushedToRemotePaths.add(writer.getFullPath());
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new LocalFileWriterFailedException(e);
                    }
                });
    }

    private Boolean shouldRotate(LocalFileWriter writer) {
        return policies.stream().reduce(false,
                (accumulated, writerPolicy) -> accumulated || writerPolicy.shouldRotate(writer), (left, right) -> left || right);
    }
}
