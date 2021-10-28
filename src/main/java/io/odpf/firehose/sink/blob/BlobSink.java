package io.odpf.firehose.sink.blob;

import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.consumer.kafka.OffsetManager;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.exception.SinkException;
import io.odpf.firehose.exception.UnknownFieldsException;
import io.odpf.firehose.exception.EmptyMessageException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.blob.message.MessageDeSerializer;
import io.odpf.firehose.sink.blob.message.Record;
import io.odpf.firehose.sink.blob.writer.WriterOrchestrator;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class BlobSink extends AbstractSink {

    private final WriterOrchestrator writerOrchestrator;
    private final OffsetManager offsetManager;
    private final MessageDeSerializer messageDeSerializer;

    private List<Message> messages;

    public BlobSink(Instrumentation instrumentation, String sinkType, OffsetManager offsetManager, WriterOrchestrator writerOrchestrator, MessageDeSerializer messageDeSerializer) {
        super(instrumentation, sinkType);
        this.offsetManager = offsetManager;
        this.writerOrchestrator = writerOrchestrator;
        this.messageDeSerializer = messageDeSerializer;
    }

    @Override
    protected List<Message> execute() throws Exception {
        List<Message> failedMessages = new LinkedList<>();
        for (Message message : messages) {
            try {
                Record record = messageDeSerializer.deSerialize(message);
                String filePath = writerOrchestrator.write(record);
                offsetManager.addOffsetToBatch(filePath, message);
            } catch (EmptyMessageException e) {
                getInstrumentation().logWarn("empty message found on topic: {}, partition: {}, offset: {}",
                        message.getTopic(), message.getPartition(), message.getOffset());
                message.setErrorInfo(new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR));
                failedMessages.add(message);
            } catch (UnknownFieldsException e) {
                getInstrumentation().logWarn(e.getMessage());
                message.setErrorInfo(new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR));
                failedMessages.add(message);
            } catch (DeserializerException e) {
                getInstrumentation().logWarn("message deserialization failed on topic: {}, partition: {}, offset: {}, reason: {}",
                        message.getTopic(), message.getPartition(), message.getOffset(), e.getMessage());
                message.setErrorInfo(new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR));
                failedMessages.add(message);
            } catch (Exception e) {
                throw new SinkException("Failed to deserialize the message", e);
            }
        }

        return failedMessages;
    }

    @Override
    protected void prepare(List<Message> messageList) throws IOException, SQLException {
        this.messages = messageList;
    }

    @Override
    public void close() throws IOException {
        writerOrchestrator.close();
    }

    @Override
    public void calculateCommittableOffsets() {
        writerOrchestrator.getFlushedPaths().forEach(offsetManager::setCommittable);
    }

    @Override
    public boolean canManageOffsets() {
        return true;
    }

    @Override
    public void addOffsetsAndSetCommittable(List<Message> messageList) {
        offsetManager.addOffsetsAndSetCommittable(messageList);
    }
}
