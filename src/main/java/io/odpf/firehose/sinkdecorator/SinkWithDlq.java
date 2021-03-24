package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This Sink pushes failed messages to kafka after retries are exhausted.
 */
public class SinkWithDlq extends SinkDecorator {

    private Producer<byte[], byte[]> kafkaProducer;
    private final String topic;
    private BackOffProvider backOffProvider;
    private Instrumentation instrumentation;

    public SinkWithDlq(Sink sink, Producer<byte[], byte[]> kafkaProducer, String topic,
                       Instrumentation instrumentation, BackOffProvider backOffProvider) {
        super(sink);
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
        this.backOffProvider = backOffProvider;
        this.instrumentation = instrumentation;
    }

    public static SinkWithDlq withInstrumentationFactory(Sink sink, Producer<byte[], byte[]> kafkaProducer, String topic,
                                                         StatsDReporter statsDReporter, BackOffProvider backOffProvider) {
        return new SinkWithDlq(sink, kafkaProducer, topic, new Instrumentation(statsDReporter, SinkWithDlq.class), backOffProvider);
    }

    /**
     * Pushes all the failed messages to kafka as DLQ.
     *
     * @param message list of messages to push
     * @return list of failed messaged
     * @throws IOException
     * @throws DeserializerException
     */
    @Override
    public List<Message> pushMessage(List<Message> message) throws IOException, DeserializerException {
        List<Message> retryQueueMessages = super.pushMessage(message);
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

    @Override
    public void close() throws IOException {
        super.close();
    }
}
