package io.odpf.firehose.sink.objectstorage.writer.remote;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.file.Path;

@AllArgsConstructor
@Data
public class ObjectStorageWriterConfig {
    private Path localBasePath;
    private String gcsBucketName;
    private String gcsProjectId;
}
