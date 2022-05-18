package io.odpf.firehose.sink.blob.writer.remote;

import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.sink.common.blobstorage.BlobStorageException;
import io.odpf.firehose.sink.blob.writer.local.LocalFileMetadata;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.odpf.firehose.metrics.Metrics.*;
import static io.odpf.firehose.metrics.Metrics.tag;
import static io.odpf.firehose.metrics.BlobStorageMetrics.*;

@AllArgsConstructor
@Data
public class BlobStorageWriterFutureHandler {
    private Future<Long> future;
    private LocalFileMetadata localFileMetadata;
    private FirehoseInstrumentation firehoseInstrumentation;
    private static final String EMPTY = "";

    public String getFullPath() {
        return localFileMetadata.getFullPath();
    }

    public boolean isFinished() {
        if (!future.isDone()) {
            return false;
        }
        try {
            long totalTime = future.get();
            captureFileUploadSuccessMetric(totalTime);
            return true;
        } catch (InterruptedException e) {
            captureUploadFailedMetric(e);
            throw new BlobStorageFailedException(e);
        } catch (ExecutionException e) {
            captureUploadFailedMetric(e.getCause());
            throw new BlobStorageFailedException(e.getCause());
        }
    }

    private void captureFileUploadSuccessMetric(long totalTime) {
        firehoseInstrumentation.logInfo("Flushed to blob storage {}", localFileMetadata.getFullPath());
        firehoseInstrumentation.incrementCounter(FILE_UPLOAD_TOTAL, SUCCESS_TAG);
        firehoseInstrumentation.captureCount(FILE_UPLOAD_BYTES, localFileMetadata.getSize());
        firehoseInstrumentation.captureCount(FILE_UPLOAD_RECORDS_TOTAL, localFileMetadata.getRecordCount());
        firehoseInstrumentation.captureDuration(FILE_UPLOAD_TIME_MILLISECONDS, totalTime);
    }

    private void captureUploadFailedMetric(Throwable e) {
        firehoseInstrumentation.logError("Failed to flush to blob storage {}", e.getMessage());
        String errorType;
        if (e instanceof BlobStorageException) {
            errorType = ((BlobStorageException) e).getErrorType();
        } else {
            errorType = "";
        }
        firehoseInstrumentation.incrementCounter(FILE_UPLOAD_TOTAL, FAILURE_TAG, tag(BLOB_STORAGE_ERROR_TYPE_TAG, errorType));
    }
}
