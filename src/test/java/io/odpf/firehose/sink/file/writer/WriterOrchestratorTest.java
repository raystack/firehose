package io.odpf.firehose.sink.file.writer;

import com.google.protobuf.StringValue;
import io.odpf.firehose.sink.file.Util;
import io.odpf.firehose.sink.file.message.KafkaMetadataUtils;
import io.odpf.firehose.sink.file.message.Record;
import io.odpf.firehose.sink.file.writer.path.TimePartitionPath;
import io.odpf.firehose.sink.file.writer.policy.SizeBasedRotatingPolicy;
import io.odpf.firehose.sink.file.writer.policy.TimeBasedRotatingPolicy;
import io.odpf.firehose.sink.file.writer.policy.WriterPolicy;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class WriterOrchestratorTest {

    public static final int DEFAULT_PAGE_SIZE = 1048576;
    public static final int DEFAULT_BLOCK_SIZE = 134217728;
    private KafkaMetadataUtils kafkaMetadataUtils = new KafkaMetadataUtils("");

    @Mock
    private TimePartitionPath timePartitionPath;

    @Mock
    private LocalFileWriter localFileWriter;

    private Path basePath;

    @Before
    public void setUp() throws Exception {
        basePath = Files.createTempDirectory("temp");
    }

    @Test
    public void shouldCreateLocalFileWriter() throws IOException {
        Record record = Util.createRecordWithMetadata("abc", "default", 1, 1, Instant.now());
        when(timePartitionPath.create(record)).thenReturn(Paths.get("dt=2021-01-01"));

        List<WriterPolicy> writerPolicies = new ArrayList<>();
        WriterOrchestrator writerOrchestrator = new WriterOrchestrator(DEFAULT_PAGE_SIZE, DEFAULT_BLOCK_SIZE, StringValue.getDescriptor(), kafkaMetadataUtils.getFieldDescriptor(), writerPolicies, timePartitionPath, WriterOrchestrator.LocalWriterType.MEMORY);
        LocalFileWriter writer = writerOrchestrator.getWriter(basePath, record);
        writerOrchestrator.close();

        verify(timePartitionPath, times(1)).create(record);
        assertNotNull(writer);
    }

    @Test
    public void shouldCreateMultipleWriterBasedOnPartition() throws IOException {
        Instant timestamp1 = Instant.parse("2020-01-01T10:00:00.000Z");
        Record record1 = Util.createRecordWithMetadata("abc", "default", 1, 1, timestamp1);
        when(timePartitionPath.create(record1)).thenReturn(Paths.get("dt=2021-01-01"));

        Instant timestamp2 = Instant.parse("2020-01-02T10:00:00.000Z");
        Record record2 = Util.createRecordWithMetadata("abc", "default", 1, 1, timestamp2);
        when(timePartitionPath.create(record2)).thenReturn(Paths.get("dt=2021-01-02"));

        List<WriterPolicy> writerPolicies = new ArrayList<>();
        WriterOrchestrator writerOrchestrator = new WriterOrchestrator(DEFAULT_PAGE_SIZE, DEFAULT_BLOCK_SIZE, StringValue.getDescriptor(), kafkaMetadataUtils.getFieldDescriptor(), writerPolicies, timePartitionPath, WriterOrchestrator.LocalWriterType.MEMORY);

        Set<LocalFileWriter> writers = new HashSet<>();

        writers.add(writerOrchestrator.getWriter(basePath, record1));
        writers.add(writerOrchestrator.getWriter(basePath, record1));
        writers.add(writerOrchestrator.getWriter(basePath, record2));
        writerOrchestrator.close();

        verify(timePartitionPath, times(3)).create(any(Record.class));
        assertEquals(2, writers.size());
    }

    @Test
    public void shouldCloseAndOpenNewWriterBasedOnARotationPolicy() throws IOException {
        Instant timestamp = Instant.parse("2020-01-01T10:00:00.000Z");
        Record record = Util.createRecordWithMetadata("abc", "default", 1, 1, timestamp);
        when(timePartitionPath.create(record)).thenReturn(Paths.get("dt=2021-01-01"));

        List<WriterPolicy> writerPolicies = new ArrayList<>();
        writerPolicies.add(new SizeBasedRotatingPolicy(2));
        WriterOrchestrator writerOrchestrator = new WriterOrchestrator(DEFAULT_PAGE_SIZE, DEFAULT_BLOCK_SIZE, StringValue.getDescriptor(), kafkaMetadataUtils.getFieldDescriptor(), writerPolicies, timePartitionPath, WriterOrchestrator.LocalWriterType.MEMORY);

        Set<LocalFileWriter> writers = new HashSet<>();

        for (int i = 0; i < 3; i++) {
            LocalFileWriter writer = writerOrchestrator.getWriter(basePath, record);
            writers.add(writer);
            writer.write(record);
        }

        writerOrchestrator.close();

        verify(timePartitionPath, times(3)).create(any(Record.class));
        assertEquals(2, writers.size());
    }

    @Test
    public void shouldCloseAndOpenNewWriterGivenMultiplePolicy() throws IOException, InterruptedException {
        Instant timestamp = Instant.parse("2020-01-01T10:00:00.000Z");
        Record record = Util.createRecordWithMetadata("abc", "default", 1, 1, timestamp);
        when(timePartitionPath.create(record)).thenReturn(Paths.get("dt=2021-01-01"));

        List<WriterPolicy> writerPolicies = new ArrayList<>();
        writerPolicies.add(new SizeBasedRotatingPolicy(10));
        writerPolicies.add(new TimeBasedRotatingPolicy(200));
        WriterOrchestrator writerOrchestrator = new WriterOrchestrator(DEFAULT_PAGE_SIZE, DEFAULT_BLOCK_SIZE, StringValue.getDescriptor(), kafkaMetadataUtils.getFieldDescriptor(), writerPolicies, timePartitionPath, WriterOrchestrator.LocalWriterType.MEMORY);

        Set<LocalFileWriter> writers = new HashSet<>();

        for (int i = 0; i < 3; i++) {
            LocalFileWriter writer = writerOrchestrator.getWriter(basePath, record);
            writers.add(writer);
            writer.write(record);
            Thread.sleep(120);
        }

        writerOrchestrator.close();

        verify(timePartitionPath, times(3)).create(any(Record.class));
        assertEquals(2, writers.size());
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenClosingWriterFailed() throws IOException {
        Record record = Util.createRecordWithMetadata("abc", "default", 1, 1, Instant.now());
        when(timePartitionPath.create(record)).thenReturn(Paths.get("dt=2021-01-01"));

        List<WriterPolicy> writerPolicies = new ArrayList<>();
        WriterOrchestrator writerOrchestrator = spy(new WriterOrchestrator(DEFAULT_PAGE_SIZE, DEFAULT_BLOCK_SIZE, StringValue.getDescriptor(), kafkaMetadataUtils.getFieldDescriptor(), writerPolicies, timePartitionPath, WriterOrchestrator.LocalWriterType.MEMORY));
        when(writerOrchestrator.createLocalFileWriter(any(Path.class))).thenReturn(localFileWriter);
        doThrow(new IOException("")).when(localFileWriter).close();

        LocalFileWriter writer = writerOrchestrator.getWriter(basePath, record);
        writerOrchestrator.close();

        verify(timePartitionPath, times(1)).create(record);
        assertNotNull(writer);
    }

    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionWhenOpenNewWriterFailed() throws IOException {
        Record record = Util.createRecordWithMetadata("abc", "default", 1, 1, Instant.now());
        when(timePartitionPath.create(record)).thenReturn(Paths.get("dt=2021-01-01"));

        List<WriterPolicy> writerPolicies = new ArrayList<>();
        WriterOrchestrator writerOrchestrator = spy(new WriterOrchestrator(DEFAULT_PAGE_SIZE, DEFAULT_BLOCK_SIZE, StringValue.getDescriptor(), kafkaMetadataUtils.getFieldDescriptor(), writerPolicies, timePartitionPath, WriterOrchestrator.LocalWriterType.MEMORY));
        when(writerOrchestrator.createLocalFileWriter(any(Path.class))).thenThrow(new IOException(""));

        LocalFileWriter writer = writerOrchestrator.getWriter(basePath, record);
        writerOrchestrator.close();

        verify(timePartitionPath, times(1)).create(record);
        assertNotNull(writer);
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(basePath.toString()));
    }
}
