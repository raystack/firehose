package io.odpf.firehose.sink.cloud;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.cloud.message.Record;
import io.odpf.firehose.sink.cloud.writer.WriterOrchestrator;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class CloudSink extends AbstractSink {

    private final WriterOrchestrator writerOrchestrator;
    private List<Record> records;

    public CloudSink(Instrumentation instrumentation, String sinkType, WriterOrchestrator writerOrchestrator) {
        super(instrumentation, sinkType);
        this.writerOrchestrator = writerOrchestrator;
    }

    @Override
    protected List<Message> execute() throws IOException {
        for (Record record : this.records) {
            this.writerOrchestrator.getWriter(record).write(record);
        }
        return new LinkedList<>();
    }

    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
        records = new LinkedList<>();
        for (Message message : messages) {
            Record record = this.writerOrchestrator.getMessageSerializer().serialize(message);
            records.add(record);
        }
    }

    @Override
    public void close() throws IOException {
        writerOrchestrator.close();
    }
}
