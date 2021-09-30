package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.Metrics;
import io.odpf.firehose.metrics.ObjectStorageMetrics;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileMetadata;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ObjectStorageWriterFutureHandlerTest {

    @Test
    public void shouldNotFilterUnfinishedFuture() {
        Future<Long> future = Mockito.mock(Future.class);
        LocalFileMetadata localFileMetadata = Mockito.mock(LocalFileMetadata.class);
        Instrumentation instrumentation = Mockito.mock(Instrumentation.class);
        ObjectStorageWriterFutureHandler handler = new ObjectStorageWriterFutureHandler(future, localFileMetadata, instrumentation);
        Mockito.when(future.isDone()).thenReturn(false);
        Assert.assertFalse(handler.isFinished());
    }

    @Test
    public void shouldFilterFinishedFuture() throws Exception {
        Future<Long> future = Mockito.mock(Future.class);
        LocalFileMetadata localFileMetadata = Mockito.mock(LocalFileMetadata.class);
        Instrumentation instrumentation = Mockito.mock(Instrumentation.class);
        ObjectStorageWriterFutureHandler handler = new ObjectStorageWriterFutureHandler(future, localFileMetadata, instrumentation);
        Mockito.when(future.isDone()).thenReturn(true);
        Mockito.when(future.get()).thenReturn(1000L);
        Mockito.when(localFileMetadata.getFullPath()).thenReturn("/tmp/test");
        Mockito.when(localFileMetadata.getSize()).thenReturn(1024L);
        Assert.assertTrue(handler.isFinished());
        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("Flushed to Object storage {}", "/tmp/test");
        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(ObjectStorageMetrics.FILE_UPLOAD_TOTAL, Metrics.SUCCESS_TAG);
        Mockito.verify(instrumentation, Mockito.times(1)).captureCount(ObjectStorageMetrics.FILE_UPLOAD_BYTES, 1024L);
        Mockito.verify(instrumentation, Mockito.times(1)).captureDuration(ObjectStorageMetrics.FILE_UPLOAD_TIME_MILLISECONDS, 1000L);
    }

    @Test
    public void shouldThrowException() throws Exception {
        Future<Long> future = Mockito.mock(Future.class);
        LocalFileMetadata localFileMetadata = Mockito.mock(LocalFileMetadata.class);
        Instrumentation instrumentation = Mockito.mock(Instrumentation.class);
        ObjectStorageWriterFutureHandler handler = new ObjectStorageWriterFutureHandler(future, localFileMetadata, instrumentation);
        Mockito.when(future.isDone()).thenReturn(true);
        Mockito.when(future.get()).thenThrow(new ExecutionException(new IOException()));
        Mockito.when(localFileMetadata.getFullPath()).thenReturn("/tmp/test");
        Mockito.when(localFileMetadata.getSize()).thenReturn(1024L);
        Assert.assertThrows(ObjectStorageFailedException.class, () -> handler.isFinished());
    }
}
