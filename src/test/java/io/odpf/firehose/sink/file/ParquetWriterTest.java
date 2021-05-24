package io.odpf.firehose.sink.file;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ParquetWriterTest {

    @Mock
    private FileWriterFactory factory;

    @Mock
    private org.apache.parquet.hadoop.ParquetWriter protoParquetWriter;

    private PathBuilder path;

    private ParquetWriter writer;
    private Record record;

    @Before
    public void setUp() throws Exception {
        DynamicMessage message = DynamicMessage.getDefaultInstance(StringValue.getDefaultInstance().getDescriptorForType());
        DynamicMessage metadata = DynamicMessage.getDefaultInstance(Int64Value.getDefaultInstance().getDescriptorForType());
        record = new Record(message, metadata);

        path = new PathBuilder().setDir(Paths.get("")).setFileName("booking");
        writer = new ParquetWriter(factory);
    }

    @Test
    public void shouldOpenWriter() throws IOException {
        when(factory.createProtoParquetWriter(path.build())).thenReturn(protoParquetWriter);
        writer.open(path);

        verify(factory).createProtoParquetWriter(path.build());
    }

    @Test(expected = IOException.class)
    public void shouldThrowExceptionWhenOpenWriterThrowIOException() throws IOException {
        IOException exception = new IOException();
        when(factory.createProtoParquetWriter(path.build())).thenThrow(exception);
        writer.open(path);
    }

    @Test
    public void shouldWrite() throws IOException {
        when(factory.createProtoParquetWriter(path.build())).thenReturn(protoParquetWriter);

        writer.open(path);
        writer.write(record);

        verify(factory).createProtoParquetWriter(path.build());
        verify(protoParquetWriter, times(1)).write(Arrays.asList(record.getMessage(), record.getMetadata()));
    }

    @Test
    public void shouldReturnDataSize() throws IOException {
        long expectedDataSize = 128L;
        when(protoParquetWriter.getDataSize()).thenReturn(expectedDataSize);
        when(factory.createProtoParquetWriter(path.build())).thenReturn(protoParquetWriter);

        writer.open(path);
        long dataSize = writer.getDataSize();

        verify(protoParquetWriter).getDataSize();
        assertEquals(expectedDataSize, dataSize);
    }

    @Test
    public void shouldCloseWriter() throws IOException {
        doNothing().when(protoParquetWriter).close();

        when(factory.createProtoParquetWriter(path.build())).thenReturn(protoParquetWriter);
        writer.open(path);
        writer.close();

        verify(protoParquetWriter).close();
    }
}