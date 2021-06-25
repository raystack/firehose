package io.odpf.firehose.sink.objectstorage;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.offset.OffsetManager;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.common.AbstractSinkWithDLQ;
import io.odpf.firehose.sink.objectstorage.message.MessageDeSerializer;
import io.odpf.firehose.sink.objectstorage.message.Record;
import io.odpf.firehose.sink.objectstorage.writer.WriterOrchestrator;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ObjectStorageSink extends AbstractSinkWithDLQ {

    public static final String DLQ_BATCH_KEY = "dlq";
    private final WriterOrchestrator writerOrchestrator;
    private final MessageDeSerializer messageDeSerializer;
    private final OffsetManager offsetManager;

    private List<Message> messages;

    public ObjectStorageSink(Instrumentation instrumentation, String sinkType, WriterOrchestrator writerOrchestrator, MessageDeSerializer messageDeSerializer, DlqWriter dlqWriter) {
        this(instrumentation, sinkType, writerOrchestrator, messageDeSerializer, new OffsetManager(), dlqWriter);
    }

    public ObjectStorageSink(Instrumentation instrumentation, String sinkType, WriterOrchestrator writerOrchestrator, MessageDeSerializer messageDeSerializer, OffsetManager offsetManager, DlqWriter dlqWriter) {
        super(instrumentation, sinkType, dlqWriter);
        this.writerOrchestrator = writerOrchestrator;
        this.offsetManager = offsetManager;
        this.messageDeSerializer = messageDeSerializer;
    }

    @Override
    protected List<Message> execute() throws Exception {
        List<Message> nonDeserializedMessages = new LinkedList<>();
        for (Message message : messages) {
            try {
                Record record = messageDeSerializer.deSerialize(message);
                offsetManager.addOffsetToBatch(writerOrchestrator.write(record), message);
            } catch (DeserializerException e) {
                nonDeserializedMessages.add(message);
            }
        }

        writeToDLQ(nonDeserializedMessages);
        return new LinkedList<>();
    }

    private void writeToDLQ(List<Message> nonDeserializedMessages) throws IOException {
        nonDeserializedMessages
                .forEach(message -> offsetManager.addOffsetToBatch(DLQ_BATCH_KEY, message));
        sendToDLQ(nonDeserializedMessages);
        if (nonDeserializedMessages.size() > 0) {
            offsetManager.commitBatch(DLQ_BATCH_KEY);
        }
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
