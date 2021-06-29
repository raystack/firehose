package io.odpf.firehose.sink.objectstorage;

import io.odpf.firehose.consumer.ErrorType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.MessageWithError;
import io.odpf.firehose.consumer.offset.OffsetManager;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.common.AbstractSinkWithDlqProcessor;
import io.odpf.firehose.sink.ExecResult;
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
import java.util.Set;
import java.util.stream.Collectors;

public class ObjectStorageSink extends AbstractSinkWithDlqProcessor {

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
    public ExecResult executeWithError() throws Exception {
        List<MessageWithError> nonDeserializedMessages = new LinkedList<>();
        for (Message message : messages) {
            try {
                Record record = messageDeSerializer.deSerialize(message);
                offsetManager.addOffsetToBatch(writerOrchestrator.write(record), message);
            } catch (DeserializerException e) {
                nonDeserializedMessages.add(new MessageWithError(message, ErrorType.DESERIALIZATION_ERROR));
            } catch (Exception e) {
                throw new DeserializerException("failed to write record", e);
            }
        }
        return new ExecResult(new LinkedList<>(), nonDeserializedMessages);
    }

    @Override
    public List<MessageWithError> processDlq(List<MessageWithError> messageWithErrors) throws IOException {
        Set<TopicPartition> batchKeys = messageWithErrors
                .stream().map(MessageWithError::getMessage).map(message -> {
                    TopicPartition topicPartition = new TopicPartition(
                            message.getTopic(),
                            message.getPartition());
                    offsetManager.addOffsetToBatch(topicPartition, message);
                    return topicPartition;
                }).collect(Collectors.toSet());

        List<MessageWithError> failedToBeProcessed = super.processDlq(messageWithErrors);
        batchKeys.forEach(offsetManager::setCommittable);
        return failedToBeProcessed;
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
    public Map<TopicPartition, OffsetAndMetadata> getCommittableOffsets() {
        writerOrchestrator.getFlushedPaths().forEach(offsetManager::setCommittable);
        return offsetManager.getCommittableOffset();
    }

    @Override
    public boolean canManageOffsets() {
        return true;
    }
}
