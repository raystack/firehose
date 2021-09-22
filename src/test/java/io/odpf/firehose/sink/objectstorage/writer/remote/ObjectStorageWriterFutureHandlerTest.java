package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.Metrics;
import io.odpf.firehose.metrics.ObjectStorageMetrics;
import io.odpf.firehose.sink.objectstorage.Constants;
import io.odpf.firehose.sink.objectstorage.writer.local.FileMeta;
import io.odpf.firehose.sink.objectstorage.writer.local.FilePartitionPath;
import io.odpf.firehose.sink.objectstorage.writer.local.FilePartitionPathConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.Future;

public class ObjectStorageWriterFutureHandlerTest {

    @Test
    public void shouldNotFilterUnfinishedFuture() {
        Future<Long> future = Mockito.mock(Future.class);
        FileMeta fileMeta = Mockito.mock(FileMeta.class);
        Instrumentation instrumentation = Mockito.mock(Instrumentation.class);
        ObjectStorageWriterFutureHandler handler = new ObjectStorageWriterFutureHandler(future, fileMeta, instrumentation);
        Mockito.when(future.isDone()).thenReturn(false);
        Assert.assertFalse(handler.isFinished());
    }

    @Test
    public void shouldFilterFinishedFuture() throws Exception {
        Future<Long> future = Mockito.mock(Future.class);
        FileMeta fileMeta = Mockito.mock(FileMeta.class);
        FilePartitionPath filePartitionPath = FilePartitionPath.parseFrom("/topic/dt=2021-01-01/hr=03", new FilePartitionPathConfig("UTC", Constants.FilePartitionType.HOUR, "dt=", "hr="));
        Instrumentation instrumentation = Mockito.mock(Instrumentation.class);
        ObjectStorageWriterFutureHandler handler = new ObjectStorageWriterFutureHandler(future, fileMeta, instrumentation);
        Mockito.when(future.isDone()).thenReturn(true);
        Mockito.when(future.get()).thenReturn(1000L);
        Mockito.when(fileMeta.getFullPath()).thenReturn("/tmp/test");
        Mockito.when(fileMeta.getFilePartitionPath()).thenReturn(filePartitionPath);
        Mockito.when(fileMeta.getFileSizeBytes()).thenReturn(1024L);
        Assert.assertTrue(handler.isFinished());
        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("Flushed to Object storage /tmp/test");
        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter(ObjectStorageMetrics.FILE_UPLOAD_TOTAL, Metrics.SUCCESS_TAG, Metrics.tag(ObjectStorageMetrics.TOPIC_TAG, "topic"));
        Mockito.verify(instrumentation, Mockito.times(1)).captureCount(ObjectStorageMetrics.FILE_UPLOAD_BYTES, 1024L, Metrics.tag(ObjectStorageMetrics.TOPIC_TAG, "topic"));
        Mockito.verify(instrumentation, Mockito.times(1)).captureDuration(ObjectStorageMetrics.FILE_UPLOAD_TIME_MILLISECONDS, 1000L, Metrics.tag(ObjectStorageMetrics.TOPIC_TAG, "topic"));
    }
}
