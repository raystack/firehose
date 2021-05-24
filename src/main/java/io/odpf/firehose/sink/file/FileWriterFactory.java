package io.odpf.firehose.sink.file;

import com.google.protobuf.Descriptors;
import lombok.AllArgsConstructor;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.IOException;
import java.util.List;

@AllArgsConstructor
public class FileWriterFactory {

    private int parquetBlockSize;
    private int parquetPageSize;
    private Descriptors.Descriptor messageDescriptor;
    private List<Descriptors.FieldDescriptor> metadataFieldDescriptor;

    // TODO: 21/05/21 fix this implementation
    public ParquetWriter createParquetWriter() throws IOException{
        return null;
    }

    // TODO: 24/05/21 fix this implementation
    public RotatingFileWriter createRotatingFileWriter() {
        return null;
    }

    public org.apache.parquet.hadoop.ParquetWriter createProtoParquetWriter(String path) throws IOException {
        return new ProtoParquetWriter(new Path(path),messageDescriptor,
                metadataFieldDescriptor,
                CompressionCodecName.GZIP,
                parquetBlockSize, parquetPageSize);
    }
}
