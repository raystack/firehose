package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.objectstorage.ObjectStorageException;
import io.odpf.firehose.sink.objectstorage.writer.local.FileMeta;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.odpf.firehose.metrics.Metrics.*;
import static io.odpf.firehose.metrics.Metrics.tag;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.*;

@AllArgsConstructor
@Data
public class ObjectStorageWriterFutureHandler {
    private Future<Long> future;
    private FileMeta fileMeta;
    private Instrumentation instrumentation;
    private static final String EMPTY = "";

    public String getFullPath() {
        return fileMeta.getFullPath();
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
            throw new ObjectStorageFailedException(e);
        } catch (ExecutionException e) {
            captureUploadFailedMetric(e.getCause());
            throw new ObjectStorageFailedException(e.getCause());
        }
    }

    private void captureFileUploadSuccessMetric(long totalTime) {
        instrumentation.logInfo("Flushed to Object storage " + fileMeta.getFullPath());
        instrumentation.incrementCounter(FILE_UPLOAD_TOTAL, SUCCESS_TAG);
        instrumentation.captureCount(FILE_UPLOAD_BYTES, fileMeta.getFileSizeBytes());
        instrumentation.captureCount(FILE_UPLOAD_RECORDS_TOTAL, fileMeta.getRecordCount());
        instrumentation.captureDuration(FILE_UPLOAD_TIME_MILLISECONDS, totalTime);
    }

    private void captureUploadFailedMetric(Throwable e) {
        instrumentation.logError("Failed to flush to Object storage {}", e.getMessage());
        String errorType;
        if (e instanceof ObjectStorageException) {
            errorType = ((ObjectStorageException) e).getErrorType();
        } else {
            errorType = "";
        }
        instrumentation.incrementCounter(FILE_UPLOAD_TOTAL, FAILURE_TAG, tag(OBJECT_STORE_ERROR_TYPE_TAG, errorType));
    }
}
