package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.objectstorage.ObjectStorage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

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

    public ObjectStorageCheckerTest() throws IOException {
    }

    @Before
    public void setup() {
        worker = new ObjectStorageChecker(
                toBeFlushedToRemotePaths,
                flushedToRemotePaths,
                remoteUploadFutures,
                remoteUploadScheduler,
                objectStorage);
    }

    @Test
    public void shouldNotAddToFlushedIfUploadIsStillRunning() {
        toBeFlushedToRemotePaths.add("/tmp/a/some-file");
        Future f = Mockito.mock(Future.class);
        Mockito.when(f.isDone()).thenReturn(false);
        Mockito.when(remoteUploadScheduler.submit(Mockito.any(Runnable.class))).thenReturn(f);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, flushedToRemotePaths.size());
        Assert.assertEquals(1, remoteUploadFutures.size());
        Assert.assertTrue(remoteUploadFutures.contains(new ObjectStorageWriterWorkerFuture(f, "/tmp/a/some-file")));
    }

    @Test
    public void shouldAddToFlushedIfUploadIsFinished() throws ExecutionException, InterruptedException {
        toBeFlushedToRemotePaths.add("/tmp/a/some-file");
        Future f = Mockito.mock(Future.class);
        Mockito.when(f.isDone()).thenReturn(false);
        Mockito.when(remoteUploadScheduler.submit(Mockito.any(Runnable.class))).thenReturn(f);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, flushedToRemotePaths.size());
        Assert.assertEquals(1, remoteUploadFutures.size());
        Assert.assertTrue(remoteUploadFutures.contains(new ObjectStorageWriterWorkerFuture(f, "/tmp/a/some-file")));

        Mockito.when(f.isDone()).thenReturn(true);
        Mockito.when(f.get()).thenReturn(null);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, remoteUploadFutures.size());
        Assert.assertEquals(1, flushedToRemotePaths.size());
        Assert.assertNotNull(flushedToRemotePaths.peek());
        Assert.assertEquals("/tmp/a/some-file", flushedToRemotePaths.peek());
    }

    @Test
    public void shouldThrowExceptionIfTheUploadIsFailed() throws ExecutionException, InterruptedException {
        expectedException.expect(ObjectStorageFailedException.class);
        toBeFlushedToRemotePaths.add("/tmp/a/some-file");
        Future f = Mockito.mock(Future.class);
        Mockito.when(f.isDone()).thenReturn(true);
        Mockito.when(f.get()).thenThrow(new ExecutionException(new IOException("failed")));
        Mockito.when(remoteUploadScheduler.submit(Mockito.any(Runnable.class))).thenReturn(f);
        worker.run();
    }
}
