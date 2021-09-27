package io.odpf.firehose.sink.objectstorage.writer;

import io.odpf.firehose.config.ObjectStorageSinkConfig;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.sink.objectstorage.Constants;
import io.odpf.firehose.sink.objectstorage.TestProtoMessage;
import io.odpf.firehose.sink.objectstorage.message.Record;
import io.odpf.firehose.sink.objectstorage.writer.local.*;
import io.odpf.firehose.sink.objectstorage.writer.local.path.TimePartitionedPathUtils;
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
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class WriterOrchestratorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private LocalFileWriter localFileWriter1;
    @Mock
    private LocalFileWriter localFileWriter2;
    @Mock
    private LocalStorage localStorage;
    @Mock
    private ObjectStorage objectStorage;
    @Mock
    private StatsDReporter statsDReporter;

    private final String zone = "UTC";
    private final String timeStampFieldName = TestProtoMessage.CREATED_TIME_FIELD_NAME;
    private final String datePrefix = "dt=";
    private final String hourPrefix = "hr=";
    private final String defaultTopic = "booking-log";
    @Mock
    private ObjectStorageSinkConfig sinkConfig;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.sinkConfig = Mockito.mock(ObjectStorageSinkConfig.class);
        Mockito.when(sinkConfig.getTimePartitioningTimeZone()).thenReturn(zone);
        Mockito.when(sinkConfig.getKafkaMetadataColumnName()).thenReturn("");
        Mockito.when(sinkConfig.getTimePartitioningFieldName()).thenReturn(timeStampFieldName);
        Mockito.when(sinkConfig.getPartitioningType()).thenReturn(Constants.FilePartitionType.HOUR);
        Mockito.when(sinkConfig.getTimePartitioningDatePrefix()).thenReturn(datePrefix);
        Mockito.when(sinkConfig.getTimePartitioningHourPrefix()).thenReturn(hourPrefix);
    }

    @Test
    public void shouldCreateLocalFileWriter() throws Exception {
        Record record = Mockito.mock(Record.class);
        Mockito.when(record.getTimestamp(timeStampFieldName)).thenReturn(Instant.ofEpochMilli(1L));
        Mockito.when(record.getTopic("")).thenReturn(defaultTopic);
        Mockito.when(localFileWriter1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp/", "/tmp/test", 0, 0, 0));
        Mockito.when(localStorage.createLocalFileWriter(TimePartitionedPathUtils.getTimePartitionedPath(record, sinkConfig))).thenReturn(localFileWriter1);
        Mockito.when(localFileWriter1.write(record)).thenReturn(true);
        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(sinkConfig, localStorage, objectStorage, statsDReporter)) {
            String path = writerOrchestrator.write(record);
            Assert.assertEquals("/tmp/test", path);
        }
    }

    @Test
    public void shouldCreateMultipleWriterBasedOnPartition() throws Exception {
        Record record1 = Mockito.mock(Record.class);
        Mockito.when(record1.getTimestamp(timeStampFieldName)).thenReturn(Instant.ofEpochMilli(3600000L));
        Mockito.when(record1.getTopic("")).thenReturn(defaultTopic);
        Mockito.when(localStorage.createLocalFileWriter(TimePartitionedPathUtils.getTimePartitionedPath(record1, sinkConfig))).thenReturn(localFileWriter1);
        Mockito.when(localFileWriter1.write(record1)).thenReturn(true);
        Mockito.when(localFileWriter1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp/", "/tmp/test1", 0, 0, 0));

        Record record2 = Mockito.mock(Record.class);
        Mockito.when(record2.getTimestamp(timeStampFieldName)).thenReturn(Instant.ofEpochMilli(7200000L));
        Mockito.when(record2.getTopic("")).thenReturn(defaultTopic);
        Mockito.when(localStorage.createLocalFileWriter(TimePartitionedPathUtils.getTimePartitionedPath(record2, sinkConfig))).thenReturn(localFileWriter2);
        Mockito.when(localFileWriter2.write(record2)).thenReturn(true);
        Mockito.when(localFileWriter2.getMetadata()).thenReturn(new LocalFileMetadata("/tmp/", "/tmp/test2", 0, 0, 0));

        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(sinkConfig, localStorage, objectStorage, statsDReporter)) {
            Set<String> paths = new HashSet<>();
            paths.add(writerOrchestrator.write(record1));
            paths.add(writerOrchestrator.write(record2));
            assertEquals(2, paths.size());
        }
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenWriteThrowsException() throws Exception {
        Record record = Mockito.mock(Record.class);
        Mockito.when(record.getTimestamp(timeStampFieldName)).thenReturn(Instant.ofEpochMilli(3600000L));
        Mockito.when(record.getTopic("")).thenReturn(defaultTopic);
        Mockito.when(localFileWriter1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp/", "/tmp/test1", 0, 0, 0));
        Mockito.when(localStorage.createLocalFileWriter(TimePartitionedPathUtils.getTimePartitionedPath(record, sinkConfig))).thenReturn(localFileWriter1);
        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(sinkConfig, localStorage, objectStorage, statsDReporter)) {
            Mockito.doThrow(new IOException("")).when(localFileWriter1).write(record);
            writerOrchestrator.write(record);
        }
    }

    @Test
    public void shouldThrowIOExceptionWhenOpenNewWriterFailed() throws Exception {
        expectedException.expect(LocalFileWriterFailedException.class);
        Record record = Mockito.mock(Record.class);
        Mockito.when(record.getTimestamp(timeStampFieldName)).thenReturn(Instant.ofEpochMilli(3600000L));
        Mockito.when(record.getTopic("")).thenReturn(defaultTopic);
        Mockito.when(localFileWriter1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp/", "/tmp/test1", 0, 0, 0));
        Mockito.when(localStorage.createLocalFileWriter(TimePartitionedPathUtils.getTimePartitionedPath(record, sinkConfig))).thenThrow(new LocalFileWriterFailedException(new IOException("Some error")));
        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(sinkConfig, localStorage, objectStorage, statsDReporter)) {
            writerOrchestrator.write(record);
        }
    }

    @Test
    public void shouldGetEmptyFlushedPath() throws Exception {
        Record record = Mockito.mock(Record.class);
        Mockito.when(record.getTimestamp(timeStampFieldName)).thenReturn(Instant.ofEpochMilli(1L));
        Mockito.when(record.getTopic("")).thenReturn(defaultTopic);
        Mockito.when(localFileWriter1.getMetadata()).thenReturn(new LocalFileMetadata("/tmp/", "/tmp/test", 0, 0, 0));
        Mockito.when(localStorage.createLocalFileWriter(TimePartitionedPathUtils.getTimePartitionedPath(record, sinkConfig))).thenReturn(localFileWriter1);
        Mockito.when(localFileWriter1.write(record)).thenReturn(true);
        try (WriterOrchestrator writerOrchestrator = new WriterOrchestrator(sinkConfig, localStorage, objectStorage, statsDReporter)) {
            String path = writerOrchestrator.write(record);
            Assert.assertEquals("/tmp/test", path);
            Assert.assertEquals(new HashSet<>(), writerOrchestrator.getFlushedPaths());
        }
    }
}
