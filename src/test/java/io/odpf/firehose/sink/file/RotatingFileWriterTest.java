package io.odpf.firehose.sink.file;

import io.odpf.firehose.sink.file.message.Record;
import io.odpf.firehose.sink.file.writer.FileWriterFactory;
import io.odpf.firehose.sink.file.writer.ParquetWriter;
import io.odpf.firehose.sink.file.writer.RotatingFileWriter;
import io.odpf.firehose.sink.file.writer.path.PathBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RotatingFileWriterTest {

    @Test
    public void shouldRotateBasedOnDuration() throws IOException, InterruptedException {
        PathBuilder base = PathBuilder.create();
        Record record = RecordsUtil.createRecord("abc", 123);

        ParquetWriter delegateWriter = mock(ParquetWriter.class);

        FileWriterFactory writerFactory = mock(FileWriterFactory.class);
        when(writerFactory.createParquetWriter()).thenReturn(delegateWriter);

        RotatingFileWriter writer = new RotatingFileWriter(300, 2, writerFactory);

        writer.open(base);
        when(delegateWriter.getDataSize()).thenReturn(6L);
        writer.write(record);
        when(delegateWriter.getDataSize()).thenReturn(12L);
        writer.write(record);

        Thread.sleep(3000);
        when(delegateWriter.getDataSize()).thenReturn(18L);
        writer.write(record);
        writer.close();

        verify(delegateWriter, times(3)).write(record);
        verify(delegateWriter, times(2)).close();
        verify(delegateWriter, times(2)).open(any(PathBuilder.class));
    }

    @Test
    public void shouldRotateBasedOnFileSize() throws IOException {
        PathBuilder base = PathBuilder.create();
        Record record = RecordsUtil.createRecord("abc", 123);

        ParquetWriter delegateWriter = mock(ParquetWriter.class);

        FileWriterFactory writerFactory = mock(FileWriterFactory.class);
        when(writerFactory.createParquetWriter()).thenReturn(delegateWriter);

        RotatingFileWriter writer = new RotatingFileWriter(10, 3600, writerFactory);

        writer.open(base);
        when(delegateWriter.getDataSize()).thenReturn(6L);
        writer.write(record);
        when(delegateWriter.getDataSize()).thenReturn(12L);
        writer.write(record);
        when(delegateWriter.getDataSize()).thenReturn(6L);
        writer.write(record);
        writer.close();

        verify(delegateWriter, times(3)).write(record);
        verify(delegateWriter, times(2)).close();
        verify(delegateWriter, times(2)).open(any(PathBuilder.class));
    }

}