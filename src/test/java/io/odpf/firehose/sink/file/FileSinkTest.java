package io.odpf.firehose.sink.file;

import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.file.message.MessageSerializer;
import io.odpf.firehose.sink.file.writer.LocalFileWriter;
import io.odpf.firehose.sink.file.writer.PartitioningWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class FileSinkTest {

    @Mock
    private MessageSerializer messageSerializer;

    @Mock
    private LocalFileWriter localFileWriter;

    @Mock
    private PartitioningWriter partitioningWriter;

    private FileSink fileSink;

    @Mock
    private Instrumentation instrumentation;

    private Path basePath = Paths.get("");

    @Before
    public void setUp() throws Exception {
        fileSink = new FileSink(instrumentation,"file" ,partitioningWriter , messageSerializer, basePath);
    }

    @Test
    public void shouldCreateWriterAndWriteRecords() {

    }

    @Test
    public void shouldThrowIOExceptionWhenCreatingFileOrWritingRecordThrowException() {

    }

    @Test
    public void shouldSerialiseMessageToRecord() {

    }

    @Test
    public void shouldThrowDeserializerExceptionWhenSerialiseThrowException() {

    }
}