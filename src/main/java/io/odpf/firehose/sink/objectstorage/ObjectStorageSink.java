package io.odpf.firehose.sink.objectstorage;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.offset.OffsetManager;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.objectstorage.message.MessageDeSerializer;
import io.odpf.firehose.sink.objectstorage.writer.WriterOrchestrator;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ObjectStorageSink extends AbstractSink {

    private final WriterOrchestrator writerOrchestrator;
    private final OffsetManager offsetManager = new OffsetManager();
    private final MessageDeSerializer messageDeSerializer;
    private List<Message> messages;

    public ObjectStorageSink(Instrumentation instrumentation, String sinkType, WriterOrchestrator writerOrchestrator, MessageDeSerializer messageDeSerializer) {
        super(instrumentation, sinkType);
        this.writerOrchestrator = writerOrchestrator;
        this.messageDeSerializer = messageDeSerializer;
    }

    @Override
    protected List<Message> execute() throws Exception {
        for (Message message : messages) {
            offsetManager.addOffsetToBatch(writerOrchestrator.write(messageDeSerializer.deSerialize(message)), message);
        }
        return new LinkedList<>();
    }

    @Override
    protected void prepare(List<Message> messageList) throws DeserializerException, IOException, SQLException {
        this.messages = messageList;
    }

    @Override
    public void close() throws IOException {
        writerOrchestrator.close();
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> getCommittableOffset() {
        writerOrchestrator.getFlushedPaths().forEach(offsetManager::commitBatch);
        return offsetManager.getCommittableOffset();
    }

    @Override
    public boolean canSyncCommit() {
        return false;
    }

}
