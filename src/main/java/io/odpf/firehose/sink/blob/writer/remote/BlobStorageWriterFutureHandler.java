package io.odpf.firehose.sink.blob.writer.remote;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.common.blobstorage.BlobStorageException;
import io.odpf.firehose.sink.blob.writer.local.LocalFileMetadata;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.odpf.firehose.metrics.BlobStorageMetrics.BLOB_STORAGE_ERROR_TYPE_TAG;
import static io.odpf.firehose.metrics.BlobStorageMetrics.FILE_UPLOAD_BYTES;
import static io.odpf.firehose.metrics.BlobStorageMetrics.FILE_UPLOAD_RECORDS_TOTAL;
import static io.odpf.firehose.metrics.BlobStorageMetrics.FILE_UPLOAD_TIME_MILLISECONDS;
import static io.odpf.firehose.metrics.BlobStorageMetrics.FILE_UPLOAD_TOTAL;
import static io.odpf.firehose.metrics.Metrics.FAILURE_TAG;
import static io.odpf.firehose.metrics.Metrics.SUCCESS_TAG;
import static io.odpf.firehose.metrics.Metrics.tag;

@AllArgsConstructor
@Data
public class BlobStorageWriterFutureHandler {
    private Future<Long> future;
    private LocalFileMetadata localFileMetadata;
    private Instrumentation instrumentation;
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
        instrumentation.logInfo("Flushed to blob storage {}", localFileMetadata.getFullPath());
        instrumentation.incrementCounter(FILE_UPLOAD_TOTAL, SUCCESS_TAG);
        instrumentation.captureCount(FILE_UPLOAD_BYTES, localFileMetadata.getSize());
        instrumentation.captureCount(FILE_UPLOAD_RECORDS_TOTAL, localFileMetadata.getRecordCount());
        instrumentation.captureDuration(FILE_UPLOAD_TIME_MILLISECONDS, totalTime);
    }

    private void captureUploadFailedMetric(Throwable e) {
        instrumentation.logError("Failed to flush to blob storage {}", e.getMessage());
        String errorType;
        if (e instanceof BlobStorageException) {
            errorType = ((BlobStorageException) e).getErrorType();
        } else {
            errorType = "";
        }
        instrumentation.incrementCounter(FILE_UPLOAD_TOTAL, FAILURE_TAG, tag(BLOB_STORAGE_ERROR_TYPE_TAG, errorType));
    }
}
