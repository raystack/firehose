package io.odpf.firehose.sink.file.writer;

import io.odpf.firehose.sink.file.message.Record;
import io.odpf.firehose.sink.file.writer.path.PathBuilder;
import io.odpf.firehose.sink.file.writer.policy.SizeBasedRotatingPolicy;
import io.odpf.firehose.sink.file.writer.policy.TimeBasedRotatingPolicy;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public class RotatingFileWriter implements FileWriter {

    private final int fileSize;
    private final int durationSeconds;
    private PathBuilder path;

    private SizeBasedRotatingPolicy sizeBasedRotatingPolicy;
    private TimeBasedRotatingPolicy timeBasedRotatingPolicy;

    private final FileWriterFactory writerFactory;
    private FileWriter fileWriter;

    public RotatingFileWriter(int fileSize, int durationSeconds, FileWriterFactory writerFactory) {
        this.fileSize = fileSize;
        this.durationSeconds = durationSeconds;
        this.writerFactory = writerFactory;
    }

    @Override
    public void open(PathBuilder path) throws IOException {
        this.path = path;
    }

    @Override
    public void write(Record record) throws IOException {
        if(fileWriter == null){
            init();
        }

        fileWriter.write(record);
        update();

        if (needRotate()) {
            fileWriter.close();
            fileWriter = null;
            init();
        }
     }

    private boolean needRotate() {
        return sizeBasedRotatingPolicy.needRotate() || timeBasedRotatingPolicy.needRotate();
    }

    private void init() throws IOException {
        this.fileWriter = createWriter();
        sizeBasedRotatingPolicy = new SizeBasedRotatingPolicy(fileSize);
        timeBasedRotatingPolicy = new TimeBasedRotatingPolicy(Duration.ofSeconds(durationSeconds));
        timeBasedRotatingPolicy.start();
    }

    @Override
    public long getDataSize() {
        return fileWriter.getDataSize();
    }

    private void update() throws IOException {
        long dataSize = getDataSize();
        this.sizeBasedRotatingPolicy.setCurrentSize(dataSize);

        Instant currentTimestamp = Instant.now();
        this.timeBasedRotatingPolicy.setRegisteredTime(currentTimestamp);
    }

    private FileWriter createWriter() throws IOException {
        ParquetWriter parquetWriter = writerFactory.createParquetWriter();

        PathBuilder generatedPath = path.copy();
        String fileName = UUID.randomUUID().toString();

        generatedPath.setFileName(fileName);

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
