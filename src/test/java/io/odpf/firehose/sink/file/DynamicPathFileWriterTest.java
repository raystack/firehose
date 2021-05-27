package io.odpf.firehose.sink.file;

import com.google.protobuf.Descriptors;
import io.odpf.firehose.sink.file.message.Record;
import io.odpf.firehose.sink.file.writer.DynamicPathFileWriter;
import io.odpf.firehose.sink.file.writer.FileWriterFactory;
import io.odpf.firehose.sink.file.writer.RotatingFileWriter;
import io.odpf.firehose.sink.file.writer.path.PathBuilder;
import io.odpf.firehose.sink.file.writer.path.TimePartitionPath;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DynamicPathFileWriterTest {

    private RotatingFileWriter delegateWriter;

    private DummyFileWriterFactory writerFactory = new DummyFileWriterFactory();

    @Mock
    private TimePartitionPath timePartitionPath;

    private DynamicPathFileWriter writer;

    private Record record = RecordsUtil.createRecord("abc", 123);

    private PathBuilder path = new PathBuilder().setDir(Paths.get("")).setFileName("booking");


    @Before
    public void setUp() throws Exception {
        delegateWriter = mock(RotatingFileWriter.class);
        writerFactory.addWriter(delegateWriter);

        writer = new DynamicPathFileWriter(timePartitionPath, writerFactory);
    }

    @Test
    public void shouldWriteRecord() throws IOException {

        Path dirPath = Paths.get("booking/2021-02-27/10");
        when(timePartitionPath.create(record)).thenReturn(dirPath);

        writer.open(path);
        writer.write(record);

        ArgumentCaptor<PathBuilder> argumentCaptor = ArgumentCaptor.forClass(PathBuilder.class);
        verify(delegateWriter).open(argumentCaptor.capture());
        verify(delegateWriter).write(record);

        assertEquals(dirPath, argumentCaptor.getValue().getDir());
    }

    @Test(expected = IOException.class)
    public void shouldThrowExceptionWhenWriteThrowException() throws IOException {
        Path dirPath = Paths.get("booking/2021-02-27/10");
        when(timePartitionPath.create(record)).thenReturn(dirPath);
        doThrow(new IOException()).when(delegateWriter).write(record);

        writer.open(path);
        writer.write(record);

        ArgumentCaptor<PathBuilder> argumentCaptor = ArgumentCaptor.forClass(PathBuilder.class);
        verify(delegateWriter).open(argumentCaptor.capture());
        verify(delegateWriter).write(record);

        assertEquals(dirPath, argumentCaptor.getValue().getDir());
    }

    @Test
    public void shouldCloseAllWriter() throws IOException {
        RotatingFileWriter otherWriter = mock(RotatingFileWriter.class);
        writerFactory.addWriter(otherWriter);

        Record otherRecord = RecordsUtil.createRecord("def",456);
        Path dirPath = Paths.get("booking/2021-02-01/10");
        Path otherDirPath = Paths.get("booking/2021-02-01/11");

        when(timePartitionPath.create(record)).thenReturn(dirPath);
        when(timePartitionPath.create(otherRecord)).thenReturn(otherDirPath);

        writer.open(path);
        writer.write(record);
        writer.write(otherRecord);
        writer.close();

        verify(delegateWriter).close();
        verify(otherWriter).close();
    }

    @After
    public void tearDown() throws Exception {
        this.writerFactory.clear();
    }

    class DummyFileWriterFactory extends FileWriterFactory {

        Queue<RotatingFileWriter> writers;

        public DummyFileWriterFactory(int parquetBlockSize, int parquetPageSize, Descriptors.Descriptor messageDescriptor, List<Descriptors.FieldDescriptor> metadataFieldDescriptor) {
            super(parquetBlockSize, parquetPageSize, messageDescriptor, metadataFieldDescriptor);
        }

        public DummyFileWriterFactory(){
            super(1,1,null,null);
            writers = new LinkedList<>();
        }

        @Override
        public RotatingFileWriter createRotatingFileWriter() {
            return writers.poll();
        }

        public void addWriter(RotatingFileWriter writer){
            this.writers.add(writer);
        }

        public void clear(){
            this.writers.clear();
        }
    }
}