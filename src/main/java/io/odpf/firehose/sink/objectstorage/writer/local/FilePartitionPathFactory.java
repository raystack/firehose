package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.sink.objectstorage.Constants;
import io.odpf.firehose.sink.objectstorage.message.Record;
import lombok.AllArgsConstructor;

import java.nio.file.Path;
import java.time.Instant;

/**
 * Create path partition from Record.
 */
@AllArgsConstructor
public class FilePartitionPathFactory {

    private final String metadataFieldName;
    private final String timeStampFieldName;
    private FilePartitionPathConfig filePartitionPathConfig;

    public Path getPartitionPath(Record record) {
        FilePartitionPath filePartitionPath = getFilePartitionPath(record);
        return filePartitionPath.getPath();
    }

    public FilePartitionPath getFilePartitionPath(Record record) {
        String topic = record.getTopic(metadataFieldName);

        Instant timestamp = null;
        if (filePartitionPathConfig.getFilePartitionType() != Constants.FilePartitionType.NONE) {
            timestamp = record.getTimestamp(timeStampFieldName);
        }

        return new FilePartitionPath(topic, timestamp, filePartitionPathConfig);
    }

    public FilePartitionPath fromFilePartitionPath(String filePartitionPath) {
        return FilePartitionPath.parseFrom(filePartitionPath, filePartitionPathConfig);
    }
}
