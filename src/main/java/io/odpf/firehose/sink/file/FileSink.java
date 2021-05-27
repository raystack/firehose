package io.odpf.firehose.sink.file;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.file.message.MessageSerializer;
import io.odpf.firehose.sink.file.message.Record;
import io.odpf.firehose.sink.file.writer.FileWriter;
import io.odpf.firehose.sink.file.writer.path.PathBuilder;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class FileSink extends AbstractSink {

    private PathBuilder path;
    private List<Record> records;
    private FileWriter writer;
    private MessageSerializer serializer;

    public FileSink(Instrumentation instrumentation, String sinkType) {
        super(instrumentation, sinkType);
        records = new LinkedList<>();
    }

    public FileSink(Instrumentation instrumentation, String sinkType, FileWriter writer, MessageSerializer serializer, PathBuilder path) {
        super(instrumentation, sinkType);
        this.writer = writer;
        this.serializer = serializer;
        this.path = path;
        this.records = new LinkedList<>();
    }

    @Override
    protected List<Message> execute() throws Exception {
        this.writer.open(path);
        for (Record record : records) {
            writer.write(record);
        }
        return new LinkedList<>();
    }

    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
        records = new LinkedList<>();
        for (Message message : messages) {
            Record record = serializer.serialize(message);
            records.add(record);
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
