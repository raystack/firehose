package io.odpf.firehose.sink.file;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
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
    private PathFactory pathFactory;

    private DynamicPathFileWriter writer;

    private Record record = createRecord("abc", 123);

    private PathBuilder path = new PathBuilder().setDir(Paths.get("")).setFileName("booking");


    @Before
    public void setUp() throws Exception {
        delegateWriter = mock(RotatingFileWriter.class);;
        writerFactory.addWriter(delegateWriter);

        writer = new DynamicPathFileWriter(pathFactory, writerFactory);
    }

    private Record createRecord(String msgValue, int msgMetadata) {
        DynamicMessage message = DynamicMessage.newBuilder(StringValue.of(msgValue)).build();
        DynamicMessage metadata = DynamicMessage.newBuilder(Int64Value.of(msgMetadata)).build();
        return new Record(message, metadata);
    }

    @Test
    public void shouldWriteRecord() throws IOException {

        Path dirPath = Paths.get("booking/2021-02-27/10");
        when(pathFactory.create(record)).thenReturn(dirPath);

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
        when(pathFactory.create(record)).thenReturn(dirPath);
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

        Record otherRecord = createRecord("def",456);
        Path dirPath = Paths.get("booking/2021-02-01/10");
        Path otherDirPath = Paths.get("booking/2021-02-01/11");

        when(pathFactory.create(record)).thenReturn(dirPath);
        when(pathFactory.create(otherRecord)).thenReturn(otherDirPath);

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

    class DummyFileWriterFactory extends FileWriterFactory{

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