package io.odpf.firehose.sinkdecorator.dlq.kafka;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.MessageWithError;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sinkdecorator.BackOffProvider;
import io.odpf.firehose.sinkdecorator.dlq.ErrorWrapperDlqWriter;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class KafkaDlqWriter extends ErrorWrapperDlqWriter {

    private Producer<byte[], byte[]> kafkaProducer;
    private final String topic;
    private final BackOffProvider backOffProvider;
    private Instrumentation instrumentation;

    public KafkaDlqWriter(Producer<byte[], byte[]> kafkaProducer, String topic, BackOffProvider backOffProvider, Instrumentation instrumentation) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
        this.backOffProvider = backOffProvider;
        this.instrumentation = instrumentation;
    }

    @Override
    public List<MessageWithError> writeWithError(List<MessageWithError> messages) throws IOException {
        List<Message> retryQueueMessages = messages.stream().map(MessageWithError::getMessage)
                .collect(Collectors.toList());
        int attemptCount = 0;

        while (!retryQueueMessages.isEmpty()) {
            instrumentation.captureRetryAttempts();
            retryQueueMessages = pushToKafka(retryQueueMessages);
            backOffProvider.backOff(attemptCount++);
        }

        return retryQueueMessages.stream().map(message -> new MessageWithError(message, null)).collect(Collectors.toList());

    }

    private ArrayList<Message> pushToKafka(List<Message> failedMessages) {
        CountDownLatch completedLatch = new CountDownLatch(1);
        AtomicInteger recordsProcessed = new AtomicInteger();
        ArrayList<Message> retryMessages = new ArrayList<>();

        instrumentation.logInfo("Pushing {} messages to retry queue topic : {}", failedMessages.size(), topic);
        for (Message message : failedMessages) {
            kafkaProducer.send(new ProducerRecord<>(topic, null, null, message.getLogKey(), message.getLogMessage(),
                    message.getHeaders()), (metadata, e) -> {
                recordsProcessed.incrementAndGet();

                if (e != null) {
                    instrumentation.incrementMessageFailCount(message, e);
                    addToFailedRecords(retryMessages, message);
                } else {
                    instrumentation.incrementMessageSucceedCount();
                }
                if (recordsProcessed.get() == failedMessages.size()) {
                    completedLatch.countDown();
                }
            });
        }
        try {
            completedLatch.await();
        } catch (InterruptedException e) {
            instrumentation.logWarn(e.getMessage());
            instrumentation.captureNonFatalError(e);
        }
        instrumentation.logInfo("Successfully pushed {} messages to {}", failedMessages.size() - retryMessages.size(), topic);
        return retryMessages;
    }

    private void addToFailedRecords(ArrayList<Message> retryMessages, Message message) {
        synchronized (this) {
            retryMessages.add(message);
        }
    }
}
