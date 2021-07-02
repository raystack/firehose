package io.odpf.firehose.objectstorage.gcs;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@Data
public class GCSConfig {

    private static final String LOCAL_BASE_PATH_KEY = "local-base-path";
    private static final String GCS_BUCKET_NAME_KEY = "gcs-bucket-name";
    private static final String CREDENTIAL_PATH_KEY = "credential-path";
    private static final String GCS_PROJECT_ID_KEY = "gcs-project-id";

    private Path localBasePath;
    private String gcsBucketName;
    private String credentialPath;
    private String gcsProjectId;

    public Map<String, Object> toMap() {
        HashMap<String, Object> properties = new HashMap<>();
        properties.put(LOCAL_BASE_PATH_KEY, localBasePath);
        properties.put(GCS_BUCKET_NAME_KEY, gcsBucketName);
        properties.put(CREDENTIAL_PATH_KEY, credentialPath);
        properties.put(GCS_PROJECT_ID_KEY, gcsProjectId);
        return properties;
    }

    public static GCSConfig create(Map<String, Object> properties) {
        Path localBasePath = (Path) properties.get(LOCAL_BASE_PATH_KEY);
        String gcsBucketName = (String) properties.get(GCS_BUCKET_NAME_KEY);
        String credentialPath = (String) properties.get(CREDENTIAL_PATH_KEY);
        String gcsProjectId = (String) properties.get(GCS_PROJECT_ID_KEY);
        return new GCSConfig(localBasePath, gcsBucketName, credentialPath, gcsProjectId);
    }
}
