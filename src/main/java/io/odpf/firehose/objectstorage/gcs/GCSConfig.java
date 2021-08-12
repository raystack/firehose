package io.odpf.firehose.objectstorage.gcs;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.file.Path;
import java.util.Properties;

@AllArgsConstructor
@Data
public class GCSConfig {

    private static final String LOCAL_BASE_PATH_KEY = "local-base-path";
    private static final String GCS_BUCKET_NAME_KEY = "gcs-bucket-name";
    private static final String CREDENTIAL_PATH_KEY = "credential-path";
    private static final String GCS_PROJECT_ID_KEY = "gcs-project-id";
    private static final String MAX_RETRY_ATTEMPT_KEY = "max-retry-attempt";
    private static final String MAX_RETRY_DURATION_KEY = "max-retry-duration";


    private Path localBasePath;
    private String gcsBucketName;
    private String credentialPath;
    private String gcsProjectId;

    private int maxRetryAttempt;
    private long maxRetryDurationMilliseconds;

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.put(LOCAL_BASE_PATH_KEY, localBasePath);
        properties.put(GCS_BUCKET_NAME_KEY, gcsBucketName);
        properties.put(CREDENTIAL_PATH_KEY, credentialPath);
        properties.put(GCS_PROJECT_ID_KEY, gcsProjectId);
        properties.put(MAX_RETRY_ATTEMPT_KEY, maxRetryAttempt);
        properties.put(MAX_RETRY_DURATION_KEY, maxRetryDurationMilliseconds);
        return properties;
    }

    public static GCSConfig create(Properties config) {
        Path localBasePath = (Path) config.get(LOCAL_BASE_PATH_KEY);
        String gcsBucketName = (String) config.get(GCS_BUCKET_NAME_KEY);
        String credentialPath = (String) config.get(CREDENTIAL_PATH_KEY);
        String gcsProjectId = (String) config.get(GCS_PROJECT_ID_KEY);
        Integer maxRetryAttempt = (Integer) config.get(MAX_RETRY_ATTEMPT_KEY);
        Long maxRetryDuration = (Long) config.get(MAX_RETRY_DURATION_KEY);
        return new GCSConfig(localBasePath, gcsBucketName, credentialPath, gcsProjectId, maxRetryAttempt, maxRetryDuration);
    }
}
