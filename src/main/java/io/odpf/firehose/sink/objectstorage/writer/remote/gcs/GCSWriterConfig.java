package io.odpf.firehose.sink.objectstorage.writer.remote.gcs;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.file.Path;

@AllArgsConstructor
@Data
public class GCSWriterConfig {
    private Path localBasePath;
    private String gcsBucketName;
    private String credentialPath;
    private String gcsProjectId;
}
