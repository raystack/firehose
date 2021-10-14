package io.odpf.firehose.sink.blob.writer.local;

import com.google.protobuf.Descriptors;
import io.odpf.firehose.sink.blob.message.Record;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class LocalParquetFileWriter implements LocalFileWriter {

    private final ParquetWriter parquetWriter;
    private final long createdTimestampMillis;
    private final String fullPath;
    private final String basePath;
    private long recordCount = 0;
    private boolean isClosed = false;

    public LocalParquetFileWriter(long createdTimestampMillis, String basePath, String fullPath, int pageSize, int blockSize, Descriptors.Descriptor messageDescriptor, List<Descriptors.FieldDescriptor> metadataFieldDescriptor) throws IOException {
        this.parquetWriter = new ProtoParquetWriter(new Path(fullPath),
                messageDescriptor,
                metadataFieldDescriptor,
                CompressionCodecName.GZIP,
                blockSize, pageSize);
        this.createdTimestampMillis = createdTimestampMillis;
        this.fullPath = fullPath;
        this.basePath = basePath;
    }

    @Override
    public LocalFileMetadata getMetadata() {
        return new LocalFileMetadata(
                basePath,
                fullPath,
                createdTimestampMillis,
                recordCount,
                parquetWriter.getDataSize());
    }

    public synchronized boolean write(Record record) throws IOException {
        if (isClosed) {
            return false;
        }
        parquetWriter.write(Arrays.asList(record.getMessage(), record.getMetadata()));
        recordCount++;
        return true;
    }

    @Override
    public synchronized void close() throws IOException {
        this.isClosed = true;
        parquetWriter.close();
    }
}
