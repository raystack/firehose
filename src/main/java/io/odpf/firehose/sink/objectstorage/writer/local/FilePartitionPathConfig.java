package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.sink.objectstorage.Constants;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class FilePartitionPathConfig {
    private String zone;
    private Constants.FilePartitionType filePartitionType;
    private String datePrefix;
    private String hourPrefix;
}
