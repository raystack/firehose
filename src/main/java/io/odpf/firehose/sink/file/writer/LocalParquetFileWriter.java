package io.odpf.firehose.sink.file.writer;

import com.google.protobuf.Descriptors;
import io.odpf.firehose.sink.file.message.Record;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class LocalParquetFileWriter implements LocalFileWriter {

    private ParquetWriter parquetWriter;
    private long createdTimestampMillis;

    public LocalParquetFileWriter(long createdTimestampMillis, String path, int pageSize, int blockSize, Descriptors.Descriptor messageDescriptor, List<Descriptors.FieldDescriptor> metadataFieldDescriptor) throws IOException {
        this.parquetWriter = new ProtoParquetWriter(new org.apache.hadoop.fs.Path(path),
                messageDescriptor,
                metadataFieldDescriptor,
                CompressionCodecName.GZIP,
                blockSize, pageSize);
        this.createdTimestampMillis = createdTimestampMillis;
    }

    public long currentSize() {
        return parquetWriter.getDataSize();
    }

    public void write(Record record) throws IOException {
        parquetWriter.write(Arrays.asList(record.getMessage(), record.getMetadata()));
    }

    @Override
    public long getCreatedTimestampMillis() {
        return this.createdTimestampMillis;
    }

    @Override
    public void close() throws IOException {
        parquetWriter.close();
    }
}
