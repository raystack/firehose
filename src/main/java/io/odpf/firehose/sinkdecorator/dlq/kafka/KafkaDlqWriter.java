package io.odpf.firehose.sinkdecorator.dlq.kafka;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sinkdecorator.BackOffProvider;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaDlqWriter implements DlqWriter {

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
    public List<Message> write(List<Message> messages) throws IOException {
        List<Message> retryQueueMessages = new LinkedList<>(messages);
        int attemptCount = 0;

        while (!retryQueueMessages.isEmpty()) {
            instrumentation.captureRetryAttempts();
            retryQueueMessages = pushToKafka(retryQueueMessages);
            backOffProvider.backOff(attemptCount++);
        }

        return retryQueueMessages;
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
