package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.objectstorage.writer.local.policy.WriterPolicy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static io.odpf.firehose.metrics.Metrics.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LocalFileCheckerTest {

    @Mock
    private LocalFileWriter writer1;

    @Mock
    private LocalFileWriter writer2;

    @Mock
    private LocalStorage localStorage;

    @Mock
    private Instrumentation instrumentation;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private BlockingQueue<String> toBeFlushedToRemotePaths = new LinkedBlockingQueue<>();
    private Map<String, LocalFileWriter> writerMap = new ConcurrentHashMap<>();
    private List<WriterPolicy> policies = new ArrayList<>();
    private LocalFileChecker worker;

    @Before
    public void setup() {
        initMocks(this);
        toBeFlushedToRemotePaths.clear();
        writerMap.clear();
        policies.clear();
        worker = new LocalFileChecker(toBeFlushedToRemotePaths, writerMap, localStorage, instrumentation);
    }

    @Test
    public void shouldRotateBasedOnPolicy() throws Exception {
        writerMap.put("/tmp/a", writer1);
        writerMap.put("/tmp/b", writer2);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(true);

        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name");
        when(writer2.getFullPath()).thenReturn("/tmp/a/random-file-name-2");
        worker.run();
        Assert.assertEquals(2, toBeFlushedToRemotePaths.size());
        Assert.assertTrue(toBeFlushedToRemotePaths.contains("/tmp/a/random-file-name"));
        Assert.assertTrue(toBeFlushedToRemotePaths.contains("/tmp/a/random-file-name-2"));
        Assert.assertEquals(0, writerMap.size());
    }

    @Test
    public void shouldRemoveFromMapIfCloseFails() throws Exception {
        expectedException.expect(LocalFileWriterFailedException.class);
        writerMap.put("/tmp/a", writer1);
        writerMap.put("/tmp/b", writer2);
        doThrow(new IOException("Failed")).when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(false);
        worker.run();
    }

    @Test
    public void shouldNotRotateBaseOnPolicy() throws Exception {
        writerMap.put("/tmp/a", writer1);
        writerMap.put("/tmp/b", writer2);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer2)).thenReturn(false);
        when(localStorage.shouldRotate(writer2)).thenReturn(false);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(2, writerMap.size());
        Assert.assertEquals(writer2, writerMap.get("/tmp/b"));
        Assert.assertEquals(writer1, writerMap.get("/tmp/a"));
    }

    @Test
    public void shouldRotateSomeBasedOnPolicy() throws Exception {
        writerMap.put("/tmp/a", writer1);
        writerMap.put("/tmp/b", writer2);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);

        when(localStorage.shouldRotate(writer2)).thenReturn(false);

        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name");
        when(writer2.getFullPath()).thenReturn("/tmp/a/random-file-name-2");
        worker.run();
        Assert.assertEquals(1, toBeFlushedToRemotePaths.size());
        Assert.assertTrue(toBeFlushedToRemotePaths.contains("/tmp/a/random-file-name"));
        Assert.assertEquals(1, writerMap.size());
        Assert.assertEquals(writer2, writerMap.get("/tmp/b"));
    }

    @Test
    public void publishMetricWhenCloseFileSuccess() throws IOException {
        writerMap.put("/tmp/a", writer1);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        long fileSize = 128L;
        when(localStorage.getFileSize("/tmp/a/random-file-name")).thenReturn(fileSize);
        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name");
        worker.run();

        verify(instrumentation).captureDurationSince(eq(SINK_OBJECTSTORAGE_LOCAL_FILE_CLOSING_TIME_MILLISECONDS), any(Instant.class));
        verify(instrumentation).incrementCounterWithTags(SINK_OBJECTSTORAGE_LOCAL_FILE_CLOSED_TOTAL, SUCCESS_TAG);
        verify(instrumentation).captureCountWithTags(SINK_OBJECTSTORAGE_LOCAL_FILE_SIZE_BYTES, fileSize);
    }

    @Test
    public void publishMetricWhenCloseFileFailed() throws IOException {
        expectedException.expect(LocalFileWriterFailedException.class);
        writerMap.put("/tmp/a", writer1);
        writerMap.put("/tmp/b", writer2);
        doThrow(new IOException("Failed")).when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(false);
        worker.run();
        verify(instrumentation).incrementCounterWithTags(SINK_OBJECTSTORAGE_LOCAL_FILE_CLOSED_TOTAL, FAILURE_TAG);
    }
}
