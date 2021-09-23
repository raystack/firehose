package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.config.ObjectStorageSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.objectstorage.Constants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static io.odpf.firehose.metrics.Metrics.*;
import static io.odpf.firehose.metrics.ObjectStorageMetrics.*;
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

    private final BlockingQueue<FileMeta> toBeFlushedToRemotePaths = new LinkedBlockingQueue<>();
    private final Map<Path, LocalFileWriter> writerMap = new ConcurrentHashMap<>();
    private LocalFileChecker worker;
    @Mock
    private ObjectStorageSinkConfig sinkConfig;

    private final long fileSize = 1024L;
    private final long recordCount = 100L;

    @Before
    public void setup() throws IOException {
        initMocks(this);
        toBeFlushedToRemotePaths.clear();
        writerMap.clear();
        when(localStorage.getFileSize(anyString())).thenReturn(fileSize);
        when(writer1.getRecordCount()).thenReturn(recordCount);
        when(writer2.getRecordCount()).thenReturn(recordCount);
        when(localStorage.getSinkConfig()).thenReturn(sinkConfig);
        when(sinkConfig.getTimePartitioningTimeZone()).thenReturn("UTC");
        when(sinkConfig.getPartitioningType()).thenReturn(Constants.FilePartitionType.HOUR);
        when(sinkConfig.getTimePartitioningDatePrefix()).thenReturn("dt=");
        when(sinkConfig.getTimePartitioningHourPrefix()).thenReturn("hr=");
        worker = new LocalFileChecker(toBeFlushedToRemotePaths, writerMap, localStorage, instrumentation);
    }

    @Test
    public void shouldRotateBasedOnPolicy() throws IOException {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        writerMap.put(Paths.get("/tmp/b"), writer2);
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(true);

        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name-1");
        when(writer2.getFullPath()).thenReturn("/tmp/b/random-file-name-2");

        worker.run();
        verify(writer1, times(1)).close();
        verify(writer2, times(1)).close();
        Assert.assertEquals(2, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, writerMap.size());
    }

    @Test
    public void shouldProduceFileMeta() throws IOException {
        long fileSize1 = 128L;
        long fileSize2 = 129L;

        writerMap.put(Paths.get("/tmp/a"), writer1);
        writerMap.put(Paths.get("/tmp/b"), writer2);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(true);

        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name");
        when(writer2.getFullPath()).thenReturn("/tmp/a/random-file-name-2");

        when(localStorage.getFileSize("/tmp/a/random-file-name")).thenReturn(fileSize1);
        when(localStorage.getFileSize("/tmp/a/random-file-name-2")).thenReturn(fileSize2);

        worker.run();
        Assert.assertEquals(2, toBeFlushedToRemotePaths.size());
        verify(writer1, times(1)).getRecordCount();
        verify(writer2, times(1)).getRecordCount();
        Assert.assertTrue(toBeFlushedToRemotePaths.contains(new FileMeta("/tmp/a/random-file-name", recordCount, fileSize1)));
        Assert.assertTrue(toBeFlushedToRemotePaths.contains(new FileMeta("/tmp/a/random-file-name-2", recordCount, fileSize2)));
        Assert.assertEquals(0, writerMap.size());
    }

    @Test
    public void shouldRemoveFromMapIfCloseFails() throws Exception {
        expectedException.expect(LocalFileWriterFailedException.class);
        writerMap.put(Paths.get("/tmp/a"), writer1);
        writerMap.put(Paths.get("/tmp/b"), writer2);
        doThrow(new IOException("Failed")).when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(false);
        worker.run();
    }

    @Test
    public void shouldNotRotateBaseOnPolicy() throws Exception {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        writerMap.put(Paths.get("/tmp/b"), writer2);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer2)).thenReturn(false);
        when(localStorage.shouldRotate(writer2)).thenReturn(false);
        worker.run();
        Assert.assertEquals(0, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(2, writerMap.size());
        Assert.assertEquals(writer2, writerMap.get(Paths.get("/tmp/b")));
        Assert.assertEquals(writer1, writerMap.get(Paths.get("/tmp/a")));
    }

    @Test
    public void shouldRotateSomeBasedOnPolicy() throws Exception {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        writerMap.put(Paths.get("/tmp/b"), writer2);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);

        when(localStorage.shouldRotate(writer2)).thenReturn(false);

        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name");
        when(writer2.getFullPath()).thenReturn("/tmp/a/random-file-name-2");
        worker.run();
        Assert.assertEquals(1, toBeFlushedToRemotePaths.size());
        Assert.assertTrue(toBeFlushedToRemotePaths.contains(new FileMeta("/tmp/a/random-file-name", recordCount, fileSize)));
        Assert.assertEquals(1, writerMap.size());
        Assert.assertEquals(writer2, writerMap.get(Paths.get("/tmp/b")));
    }

    @Test
    public void shouldRecordMetricOfSuccessfullyClosedFiles() throws IOException {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name");
        worker.run();
        verify(instrumentation).incrementCounter(LOCAL_FILE_CLOSE_TOTAL, SUCCESS_TAG);
    }

    @Test
    public void shouldRecordMetricOfClosingTimeDuration() throws IOException {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name");
        worker.run();

        verify(instrumentation, times(1)).captureDurationSince(eq(LOCAL_FILE_CLOSING_TIME_MILLISECONDS), any(Instant.class));
    }

    @Test
    public void shouldRecordMetricOfFileSizeInBytes() throws IOException {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        doNothing().when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name");
        worker.run();

        verify(instrumentation, times(1)).captureCount(LOCAL_FILE_SIZE_BYTES, fileSize);
    }

    @Test
    public void shouldRecordMetricOfFailedClosedFiles() throws IOException {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        writerMap.put(Paths.get("/tmp/b"), writer2);
        doThrow(new IOException("Failed")).when(writer1).close();
        when(localStorage.shouldRotate(writer1)).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(false);

        try {
            worker.run();
        } catch (LocalFileWriterFailedException ignored) {
        }

        verify(instrumentation, times(1)).incrementCounter(LOCAL_FILE_CLOSE_TOTAL, FAILURE_TAG);
    }

    @Test
    public void shouldCaptureValueOfFileOpenCount() throws IOException {
        writerMap.put(Paths.get("/tmp/a"), writer1);
        writerMap.put(Paths.get("/tmp/b"), writer2);
        when(localStorage.shouldRotate(writer1)).thenReturn(false).thenReturn(true);
        when(localStorage.shouldRotate(writer2)).thenReturn(false).thenReturn(true);

        when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name-1");
        when(writer2.getFullPath()).thenReturn("/tmp/b/random-file-name-2");

        worker.run();
        worker.run();
        worker.run();

        verify(writer1, times(1)).close();
        verify(writer2, times(1)).close();
        Assert.assertEquals(2, toBeFlushedToRemotePaths.size());
        Assert.assertEquals(0, writerMap.size());
        verify(instrumentation, times(3)).captureValue(LOCAL_FILE_OPEN_TOTAL, 2);
        verify(instrumentation, times(3)).captureValue(LOCAL_FILE_OPEN_TOTAL, 0);
    }
}
