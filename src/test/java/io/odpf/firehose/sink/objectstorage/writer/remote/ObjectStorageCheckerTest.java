package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalStorage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import static io.odpf.firehose.metrics.Metrics.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ObjectStorageCheckerTest {

    private final BlockingQueue<String> toBeFlushedToRemotePaths = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> flushedToRemotePaths = new LinkedBlockingQueue<>();
    private final ExecutorService remoteUploadScheduler = Mockito.mock(ExecutorService.class);
    private final Set<ObjectStorageWriterWorkerFuture> remoteUploadFutures = new HashSet<>();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private ObjectStorage objectStorage;
    private ObjectStorageChecker worker;

    @Mock
    private LocalStorage localStorage;

    @Mock
    private Instrumentation instrumentation;

    public ObjectStorageCheckerTest() throws IOException {
    }

    @Before
    public void setup() {
        worker = new ObjectStorageChecker(
                toBeFlushedToRemotePaths,
                flushedToRemotePaths,
                remoteUploadFutures,
                remoteUploadScheduler,
                objectStorage,
                localStorage,
                instrumentation);
    }

    @Test
    public void shouldNotAddToFlushedIfUploadIsStillRunning() {
        String path = "/tmp/a/some-file";
        toBeFlushedToRemotePaths.add(path);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(false);
        when(remoteUploadScheduler.submit(any(Runnable.class))).thenReturn(f);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, flushedToRemotePaths.size());
        Assert.assertEquals(1, remoteUploadFutures.size());
        ArrayList<ObjectStorageWriterWorkerFuture> workerFutures = new ArrayList<>(remoteUploadFutures);
        assertEquals(f, workerFutures.get(0).getFuture());
        assertEquals(path, workerFutures.get(0).getPath());
    }

    @Test
    public void shouldAddToFlushedIfUploadIsFinished() throws ExecutionException, InterruptedException {
        String path = "/tmp/a/some-file";
        toBeFlushedToRemotePaths.add(path);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(false);
        when(remoteUploadScheduler.submit(any(Runnable.class))).thenReturn(f);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, flushedToRemotePaths.size());
        Assert.assertEquals(1, remoteUploadFutures.size());

        ArrayList<ObjectStorageWriterWorkerFuture> workerFutures = new ArrayList<>(remoteUploadFutures);
        assertEquals(f, workerFutures.get(0).getFuture());
        assertEquals(path, workerFutures.get(0).getPath());

        when(f.isDone()).thenReturn(true);
        when(f.get()).thenReturn(null);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, remoteUploadFutures.size());
        Assert.assertEquals(1, flushedToRemotePaths.size());
        Assert.assertNotNull(flushedToRemotePaths.peek());
        Assert.assertEquals(path, flushedToRemotePaths.peek());
    }

    @Test
    public void shouldThrowExceptionIfTheUploadIsFailed() throws ExecutionException, InterruptedException {
        expectedException.expect(ObjectStorageFailedException.class);
        toBeFlushedToRemotePaths.add("/tmp/a/some-file");
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(true);
        when(f.get()).thenThrow(new ExecutionException(new IOException("failed")));
        when(remoteUploadScheduler.submit(any(Runnable.class))).thenReturn(f);
        worker.run();
    }

    @Test
    public void shouldRecordMetricWhenUploadSuccess() throws IOException {
        String path = "/tmp/a/some-file";
        toBeFlushedToRemotePaths.add(path);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(true);
        when(remoteUploadScheduler.submit(any(Runnable.class))).thenReturn(f);
        when(localStorage.getFileSize(path)).thenReturn(128L);
        worker.run();

        verify(instrumentation).incrementCounterWithTags(SINK_OBJECTSTORAGE_FILE_UPLOAD_TOTAL, SUCCESS_TAG);
        verify(instrumentation).captureCountWithTags(SINK_OBJECTSTORAGE_FILE_UPLOAD_BYTES, 128L);
        verify(instrumentation).captureDurationSince(eq(SINK_OBJECTSTORAGE_FILE_UPLOAD_TIME_MILLISECONDS), any(Instant.class));
    }

    @Test
    public void shouldThrowExceptionWhenFailedToObtainFileSize() throws IOException {
        String path = "/tmp/a/some-file";
        toBeFlushedToRemotePaths.add(path);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(true);
        when(remoteUploadScheduler.submit(any(Runnable.class))).thenReturn(f);
        when(localStorage.getFileSize(path)).thenThrow(new IOException());

        Throwable cause = null;
        try {
            worker.run();
        } catch (ObjectStorageFailedException e) {
            cause = e.getCause();
        }

        assertEquals(IOException.class, cause.getClass());
    }

    @Test
    public void shouldRecordMetricWhenUploadFailed() throws ExecutionException, InterruptedException {
        String path = "/tmp/a/some-file";
        toBeFlushedToRemotePaths.add(path);
        Future f = Mockito.mock(Future.class);
        when(f.isDone()).thenReturn(true);
        when(f.get()).thenThrow(new InterruptedException());
        when(remoteUploadScheduler.submit(any(Runnable.class))).thenReturn(f);
        try {
            worker.run();
        } catch (ObjectStorageFailedException e) {
            e.printStackTrace();
        }

        verify(instrumentation).incrementCounterWithTags(SINK_OBJECTSTORAGE_FILE_UPLOAD_TOTAL, FAILURE_TAG);
    }
}
