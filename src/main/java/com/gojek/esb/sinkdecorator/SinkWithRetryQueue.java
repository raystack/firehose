package com.gojek.esb.sinkdecorator;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.Sink;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class SinkWithRetryQueue extends SinkDecorator {

    private Producer<byte[], byte[]> kafkaProducer;
    private final String topic;
    private BackOffProvider backOffProvider;
    private Instrumentation instrumentation;

    public SinkWithRetryQueue(Sink sink, Producer<byte[], byte[]> kafkaProducer, String topic,
            Instrumentation instrumentation, BackOffProvider backOffProvider) {
        super(sink);
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
        this.backOffProvider = backOffProvider;
        this.instrumentation = instrumentation;
    }

    public static SinkWithRetryQueue withInstrumentationFactory(Sink sink, Producer<byte[], byte[]> kafkaProducer, String topic,
    StatsDReporter statsDReporter, BackOffProvider backOffProvider) {
        Instrumentation instrumentation = new Instrumentation(statsDReporter, SinkWithRetryQueue.class);
        return new SinkWithRetryQueue(sink, kafkaProducer, topic, instrumentation, backOffProvider);
    }

    @Override
    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessage) throws IOException, DeserializerException {
        List<EsbMessage> retryQueueMessages = super.pushMessage(esbMessage);
        int attemptCount = 0;

        while (!retryQueueMessages.isEmpty()) {
            instrumentation.captureRetryAttempts();
            retryQueueMessages = pushToKafka(retryQueueMessages);
            backOffProvider.backOff(attemptCount++);
        }

        return retryQueueMessages;
    }

    private ArrayList<EsbMessage> pushToKafka(List<EsbMessage> failedMessages) {
        CountDownLatch completedLatch = new CountDownLatch(1);
        AtomicInteger recordsProcessed = new AtomicInteger();
        ArrayList<EsbMessage> retryMessages = new ArrayList<>();

        instrumentation.logInfo("Pushing {} messages to retry queue topic : {}", failedMessages.size(), topic);
        for (EsbMessage message : failedMessages) {
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
            instrumentation.captureNonFatalError(e);
        }
        instrumentation.logInfo("Successfully pushed {} messages to {}", failedMessages.size() - retryMessages.size(), topic);
        return retryMessages;
    }

    private void addToFailedRecords(ArrayList<EsbMessage> retryMessages, EsbMessage message) {
        synchronized (this) {
            retryMessages.add(message);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
