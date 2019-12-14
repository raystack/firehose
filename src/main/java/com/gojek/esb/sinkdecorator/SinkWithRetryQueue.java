package com.gojek.esb.sinkdecorator;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.Sink;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.gojek.esb.metrics.Metrics.*;

public class SinkWithRetryQueue extends SinkDecorator {

    private Producer<byte[], byte[]> kafkaProducer;
    private final String topic;
    private StatsDReporter statsDReporter;
    private BackOffProvider backOffProvider;
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkWithRetryQueue.class);

    public SinkWithRetryQueue(Sink sink, Producer<byte[], byte[]> kafkaProducer, String topic,
            StatsDReporter statsDReporter, BackOffProvider backOffProvider) {
        super(sink);
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
        this.statsDReporter = statsDReporter;
        this.backOffProvider = backOffProvider;
    }

    @Override
    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessage) throws IOException, DeserializerException {
        List<EsbMessage> retryQueueMessages = super.pushMessage(esbMessage);
        int attemptCount = 0;

        while (!retryQueueMessages.isEmpty()) {
            this.statsDReporter.increment(RETRY_ATTEMPTS);
            retryQueueMessages = pushToKafka(retryQueueMessages);
            backOffProvider.backOff(attemptCount++);
        }

        return retryQueueMessages;
    }

    private ArrayList<EsbMessage> pushToKafka(List<EsbMessage> failedMessages) {
        CountDownLatch completedLatch = new CountDownLatch(1);
        AtomicInteger recordsProcessed = new AtomicInteger();
        ArrayList<EsbMessage> retryMessages = new ArrayList<>();

        LOGGER.info("Pushing {} messages to retry queue topic : {}", failedMessages.size(), topic);
        for (EsbMessage message : failedMessages) {
            kafkaProducer.send(new ProducerRecord<>(topic, null, null, message.getLogKey(), message.getLogMessage(),
                    message.getHeaders()), (metadata, e) -> {
                        recordsProcessed.incrementAndGet();

                        if (e != null) {
                            this.statsDReporter.increment(RETRY_MESSAGE_COUNT, FAILURE_TAG);
                            LOGGER.error("Unable to send record with key " + message.getLogKey() + " and message "
                                    + message.getLogMessage(), e);
                            addToFailedRecords(retryMessages, message);
                        } else {
                            this.statsDReporter.increment(RETRY_MESSAGE_COUNT, SUCCESS_TAG);
                        }
                        if (recordsProcessed.get() == failedMessages.size()) {
                            completedLatch.countDown();
                        }
                    });
        }
        try {
            completedLatch.await();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e.getClass());
        }
        LOGGER.info("Successfully pushed {} messages to {}", failedMessages.size() - retryMessages.size(), topic);
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
