package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.sink.objectstorage.writer.local.policy.WriterPolicy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static org.mockito.MockitoAnnotations.initMocks;

public class LocalFileCheckerTest {

    @Mock
    private LocalFileWriter writer1;

    @Mock
    private LocalFileWriter writer2;

    @Mock
    private WriterPolicy policy1;

    @Mock
    private WriterPolicy policy2;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private BlockingQueue<String> toBeFlushedToRemotePaths = new LinkedBlockingQueue<>();
    private Map<String, LocalFileWriter> writerMap = new ConcurrentHashMap<>();
    private List<WriterPolicy> policies = new ArrayList<>();
    private LocalFileChecker worker = new LocalFileChecker(toBeFlushedToRemotePaths, writerMap, policies);

    @Before
    public void setup() {
        initMocks(this);
        toBeFlushedToRemotePaths.clear();
        writerMap.clear();
        policies.clear();
    }

    @Test
    public void shouldRotateBasedOnPolicy() throws Exception {
        writerMap.put("/tmp/a", writer1);
        writerMap.put("/tmp/b", writer2);
        policies.add(policy1);
        policies.add(policy2);
        Mockito.doNothing().when(writer1).close();
        Mockito.when(policy1.shouldRotate(writer1)).thenReturn(true);
        Mockito.when(policy2.shouldRotate(writer1)).thenReturn(false);

        Mockito.when(policy1.shouldRotate(writer2)).thenReturn(false);
        Mockito.when(policy2.shouldRotate(writer2)).thenReturn(true);

        Mockito.when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name");
        Mockito.when(writer2.getFullPath()).thenReturn("/tmp/a/random-file-name-2");
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
        policies.add(policy1);
        Mockito.doThrow(new IOException("Failed")).when(writer1).close();
        Mockito.when(policy1.shouldRotate(writer1)).thenReturn(true);
        Mockito.when(policy1.shouldRotate(writer2)).thenReturn(false);
        worker.run();
    }

    @Test
    public void shouldNotRotateBaseOnPolicy() throws Exception {
        writerMap.put("/tmp/a", writer1);
        writerMap.put("/tmp/b", writer2);
        policies.add(policy1);
        policies.add(policy2);
        Mockito.doNothing().when(writer1).close();
        Mockito.when(policy1.shouldRotate(writer1)).thenReturn(false);
        Mockito.when(policy2.shouldRotate(writer1)).thenReturn(false);
        Mockito.when(policy1.shouldRotate(writer2)).thenReturn(false);
        Mockito.when(policy2.shouldRotate(writer2)).thenReturn(false);
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
        policies.add(policy1);
        policies.add(policy2);
        Mockito.doNothing().when(writer1).close();
        Mockito.when(policy1.shouldRotate(writer1)).thenReturn(true);
        Mockito.when(policy2.shouldRotate(writer1)).thenReturn(false);

        Mockito.when(policy1.shouldRotate(writer2)).thenReturn(false);
        Mockito.when(policy2.shouldRotate(writer2)).thenReturn(false);

        Mockito.when(writer1.getFullPath()).thenReturn("/tmp/a/random-file-name");
        Mockito.when(writer2.getFullPath()).thenReturn("/tmp/a/random-file-name-2");
        worker.run();
        Assert.assertEquals(1, toBeFlushedToRemotePaths.size());
        Assert.assertTrue(toBeFlushedToRemotePaths.contains("/tmp/a/random-file-name"));
        Assert.assertEquals(1, writerMap.size());
        Assert.assertEquals(writer2, writerMap.get("/tmp/b"));

    }
}
