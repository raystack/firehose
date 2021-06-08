package io.odpf.firehose.sink.objectstorage.writer.remote;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.*;

public class ObjectStorageFileCheckerWorkerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private final BlockingQueue<String> toBeFlushedToRemotePaths = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> flushedToRemotePaths = new LinkedBlockingQueue<>();
    private final ExecutorService remoteUploadScheduler = Mockito.mock(ExecutorService.class);
    private final BlockingQueue<ObjectStorageWriterWorkerFuture> remoteUploadFutures = new LinkedBlockingQueue<>();
    private ObjectStorageFileCheckerWorker worker;

    @Before
    public void setup() {
        worker = new ObjectStorageFileCheckerWorker(
                toBeFlushedToRemotePaths,
                flushedToRemotePaths,
                remoteUploadFutures,
                remoteUploadScheduler);
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
        Assert.assertNotNull(remoteUploadFutures.peek());
        Assert.assertEquals("/tmp/a/some-file", remoteUploadFutures.peek().getPath());
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
        Assert.assertNotNull(remoteUploadFutures.peek());
        Assert.assertEquals("/tmp/a/some-file", remoteUploadFutures.peek().getPath());

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
        expectedException.expect(ObjectStorageUploadFailedException.class);
        toBeFlushedToRemotePaths.add("/tmp/a/some-file");
        Future f = Mockito.mock(Future.class);
        Mockito.when(f.isDone()).thenReturn(true);
        Mockito.when(f.get()).thenThrow(new ExecutionException(new IOException("failed")));
        Mockito.when(remoteUploadScheduler.submit(Mockito.any(Runnable.class))).thenReturn(f);
        worker.run();
    }


}
