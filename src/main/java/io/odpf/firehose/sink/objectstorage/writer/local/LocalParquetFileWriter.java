package io.odpf.firehose.sink.objectstorage.writer.local;

import com.google.protobuf.Descriptors;
import io.odpf.firehose.sink.objectstorage.message.Record;
import lombok.Getter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class LocalParquetFileWriter implements LocalFileWriter {

    private ParquetWriter parquetWriter;
    private long createdTimestampMillis;
    @Getter
    private String fullPath;
    private AtomicLong recordCount = new AtomicLong();

    public LocalParquetFileWriter(long createdTimestampMillis, String path, int pageSize, int blockSize, Descriptors.Descriptor messageDescriptor, List<Descriptors.FieldDescriptor> metadataFieldDescriptor) throws IOException {
        this.parquetWriter = new ProtoParquetWriter(new org.apache.hadoop.fs.Path(path),
                messageDescriptor,
                metadataFieldDescriptor,
                CompressionCodecName.GZIP,
                blockSize, pageSize);
        this.createdTimestampMillis = createdTimestampMillis;
        this.fullPath = path;
    }

    public long currentSize() {
        return parquetWriter.getDataSize();
    }

    public void write(Record record) throws IOException {
        parquetWriter.write(Arrays.asList(record.getMessage(), record.getMetadata()));
        recordCount.incrementAndGet();
    }

    @Override
    public long getCreatedTimestampMillis() {
        return this.createdTimestampMillis;
    }

    @Override
    public Long getRecordCount() {
        return recordCount.get();
    }

    @Override
    public void close() throws IOException {
        parquetWriter.close();
    }
}
