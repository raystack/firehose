package io.odpf.firehose.sink.objectstorage.writer;

import io.odpf.firehose.sink.objectstorage.TestUtils;
import io.odpf.firehose.sink.objectstorage.message.Record;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileWriter;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalFileWriterFailedException;
import io.odpf.firehose.sink.objectstorage.writer.local.LocalStorage;
import io.odpf.firehose.sink.objectstorage.writer.local.TimePartitionPath;
import io.odpf.firehose.objectstorage.ObjectStorage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class WriterOrchestratorTest {


    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private TimePartitionPath timePartitionPath;
    @Mock
    private LocalFileWriter localFileWriter1;
    @Mock
    private LocalFileWriter localFileWriter2;
    @Mock
    private LocalStorage writerWrapper;
    @Mock
    private ObjectStorage objectStorage;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldCreateLocalFileWriter() throws Exception {
        Record record = TestUtils.createRecordWithMetadata("abc", "default", 1, 1, Instant.now());
        Mockito.when(timePartitionPath.create(record)).thenReturn(Paths.get("dt=2021-01-01"));
        Mockito.when(localFileWriter1.getFullPath()).thenReturn("test");
        Mockito.when(writerWrapper.getTimePartitionPath()).thenReturn(timePartitionPath);
        Mockito.when(writerWrapper.getPolicies()).thenReturn(new ArrayList<>());
        Mockito.when(writerWrapper.createLocalFileWriter(Paths.get("dt=2021-01-01"))).thenReturn(localFileWriter1);
        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(writerWrapper, objectStorage)) {
            String path = writerOrchestrator.write(record);
            Mockito.verify(timePartitionPath, Mockito.times(1)).create(record);
            Assert.assertEquals("test", path);
        }
    }

    @Test
    public void shouldCreateMultipleWriterBasedOnPartition() throws Exception {
        Instant timestamp1 = Instant.parse("2020-01-01T10:00:00.000Z");
        Record record1 = TestUtils.createRecordWithMetadata("abc", "default", 1, 1, timestamp1);
        Mockito.when(timePartitionPath.create(record1)).thenReturn(Paths.get("dt=2021-01-01"));
        Mockito.when(writerWrapper.getTimePartitionPath()).thenReturn(timePartitionPath);
        Mockito.when(writerWrapper.getPolicies()).thenReturn(new ArrayList<>());

        Mockito.when(localFileWriter1.getFullPath()).thenReturn("test1");
        Mockito.when(writerWrapper.createLocalFileWriter(Paths.get("dt=2021-01-01"))).thenReturn(localFileWriter1);

        Instant timestamp2 = Instant.parse("2020-01-02T10:00:00.000Z");
        Record record2 = TestUtils.createRecordWithMetadata("abc", "default", 1, 1, timestamp2);
        Mockito.when(timePartitionPath.create(record2)).thenReturn(Paths.get("dt=2021-01-02"));

        Mockito.when(localFileWriter2.getFullPath()).thenReturn("test2");
        Mockito.when(writerWrapper.createLocalFileWriter(Paths.get("dt=2021-01-02"))).thenReturn(localFileWriter2);

        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(writerWrapper, objectStorage)) {
            Set<String> paths = new HashSet<>();
            paths.add(writerOrchestrator.write(record1));
            paths.add(writerOrchestrator.write(record1));
            paths.add(writerOrchestrator.write(record2));
            Mockito.verify(timePartitionPath, Mockito.times(3)).create(any(Record.class));
            assertEquals(2, paths.size());
        }
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenWriteThrowsException() throws Exception {
        Record record = TestUtils.createRecordWithMetadata("abc", "default", 1, 1, Instant.now());
        Mockito.when(timePartitionPath.create(record)).thenReturn(Paths.get("dt=2021-01-01"));
        Mockito.when(writerWrapper.getTimePartitionPath()).thenReturn(timePartitionPath);
        Mockito.when(writerWrapper.getPolicies()).thenReturn(new ArrayList<>());
        Mockito.when(localFileWriter1.getFullPath()).thenReturn("test1");
        Mockito.when(writerWrapper.createLocalFileWriter(Paths.get("dt=2021-01-01"))).thenReturn(localFileWriter1);
        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(writerWrapper, objectStorage)) {
            Mockito.doThrow(new IOException("")).when(localFileWriter1).write(record);
            writerOrchestrator.write(record);
        }
    }

    @Test
    public void shouldThrowIOExceptionWhenOpenNewWriterFailed() throws Exception {
        expectedException.expect(LocalFileWriterFailedException.class);
        Record record = TestUtils.createRecordWithMetadata("abc", "default", 1, 1, Instant.now());
        Mockito.when(timePartitionPath.create(record)).thenReturn(Paths.get("dt=2021-01-01"));
        Mockito.when(writerWrapper.getTimePartitionPath()).thenReturn(timePartitionPath);
        Mockito.when(writerWrapper.getPolicies()).thenReturn(new ArrayList<>());
        Mockito.when(localFileWriter1.getFullPath()).thenReturn("test1");
        Mockito.when(writerWrapper.createLocalFileWriter(Paths.get("dt=2021-01-01"))).thenThrow(new LocalFileWriterFailedException(new IOException("Some error")));
        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(writerWrapper, objectStorage)) {
            writerOrchestrator.write(record);
        }
    }

    @After
    public void tearDown() throws Exception {
    }
}
