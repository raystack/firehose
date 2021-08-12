package io.odpf.firehose.objectstorage.gcs.exception;

import io.odpf.firehose.objectstorage.gcs.error.GCSErrorType;
import lombok.AllArgsConstructor;
import lombok.Getter;


@AllArgsConstructor
@Getter
public class GCSException extends Exception {
    private GCSErrorType errorType;
    private int errorCode;
    private String reason;
}
