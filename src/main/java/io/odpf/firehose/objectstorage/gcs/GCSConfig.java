package io.odpf.firehose.objectstorage.gcs;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.file.Path;

@AllArgsConstructor
@Data
public class GCSConfig {
    private Path localBasePath;
    private String gcsBucketName;
    private String credentialPath;
    private String gcsProjectId;
}
