package io.odpf.firehose.sink.file;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

// TODO: 21/05/21 test this
public class RotatingFileWriter implements FileWriter {

    private PathBuilder path;

    private SizeBasedRotatingPolicy sizeBasedRotatingPolicy;
    private TimeBasedRotatingPolicy timeBasedRotatingPolicy;

    private final FileWriterFactory writerFactory;
    private FileWriter fileWriter;

    public RotatingFileWriter(FileWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
    }

    @Override
    public void open(PathBuilder path) throws IOException {
        this.path = path;
    }

    @Override
    public void write(Record record) throws IOException {
        if (fileWriter == null) {
            init();
        }

        fileWriter.write(record);
        update();

        if (sizeBasedRotatingPolicy.needRotate() || timeBasedRotatingPolicy.needRotate()) {
            init();
        }
    }

    private void init() throws IOException {
        this.fileWriter = createWriter();
        sizeBasedRotatingPolicy = new SizeBasedRotatingPolicy(256);
        timeBasedRotatingPolicy = new TimeBasedRotatingPolicy(Duration.ofHours(1));
    }

    @Override
    public long getDataSize() {
        return fileWriter.getDataSize();
    }

    private void update() {
        long dataSize = getDataSize();
        this.sizeBasedRotatingPolicy.setCurrentSize(dataSize);

        Instant currentTimestamp = Instant.now();
        this.timeBasedRotatingPolicy.setRegisteredTime(currentTimestamp);
    }

    private FileWriter createWriter() throws IOException {
        ParquetWriter parquetWriter = writerFactory.createParquetWriter();

        // TODO: 24/05/21 implement path generation here
        PathBuilder generatedPath = path.copy();
        parquetWriter.open(generatedPath);
        return parquetWriter;
    }

    @Override
    public void close() throws IOException {
        if (fileWriter != null) {
            fileWriter.close();
        }
    }
}
