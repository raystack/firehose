package io.odpf.firehose.sink.file.writer;

import com.google.protobuf.Descriptors;
import io.odpf.firehose.sink.file.message.Record;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Getter
public class LocalFileWriter implements Closeable {

    private ParquetWriter parquetWriter;
    private long createdTimestampMillis;

    public LocalFileWriter(long createdTimestampMillis, String path, int pageSize, int blockSize, Descriptors.Descriptor messageDescriptor, List<Descriptors.FieldDescriptor> metadataFieldDescriptor) throws IOException {
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
    public void close() throws IOException {
        parquetWriter.close();
    }
}
