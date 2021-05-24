package io.odpf.firehose.sink.file;

import java.io.IOException;
import java.util.Arrays;

public class ParquetWriter implements FileWriter {

    private FileWriterFactory factory;
    private org.apache.parquet.hadoop.ParquetWriter parquetWriter;

    public ParquetWriter(FileWriterFactory factory) {
        this.factory = factory;
    }

    @Override
    public void open(PathBuilder path) throws IOException {
        String fullPath = path.build();
        parquetWriter = factory.createProtoParquetWriter(fullPath);
    }

    public void write(Record record) throws IOException {
        parquetWriter.write(Arrays.asList(record.getMessage(), record.getMetadata()));
    }

    @Override
    public long getDataSize() {
        return parquetWriter.getDataSize();
    }

    public void close() throws IOException {
        parquetWriter.close();
    }
}
