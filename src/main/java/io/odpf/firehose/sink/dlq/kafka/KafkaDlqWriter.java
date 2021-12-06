package io.odpf.firehose.sink.dlq.kafka;

import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.dlq.DlqWriter;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaDlqWriter implements DlqWriter {

    private Producer<byte[], byte[]> kafkaProducer;
    private final String topic;
    private Instrumentation instrumentation;

    public KafkaDlqWriter(Producer<byte[], byte[]> kafkaProducer, String topic, Instrumentation instrumentation) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
        this.instrumentation = instrumentation;
    }

    @Override
    public List<Message> write(List<Message> messages) throws IOException {
        if (messages.isEmpty()) {
            return Collections.emptyList();
        }
        CountDownLatch completedLatch = new CountDownLatch(1);
        AtomicInteger recordsProcessed = new AtomicInteger();
        List<Message> failedMessages = new ArrayList<>();

        instrumentation.logInfo("Pushing {} messages to retry queue topic : {}", messages.size(), topic);
        for (Message message : messages) {
            kafkaProducer.send(new ProducerRecord<>(topic, null, null, message.getLogKey(), message.getLogMessage(),
                    message.getHeaders()), (metadata, e) -> {
                recordsProcessed.incrementAndGet();

                if (e != null) {
                    synchronized (failedMessages) {
                        failedMessages.add(message);
                    }
                }
                if (recordsProcessed.get() == messages.size()) {
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
        instrumentation.logInfo("Successfully pushed {} messages to {}", messages.size() - failedMessages.size(), topic);
        return failedMessages;
    }
}
