package io.odpf.firehose.consumer;
import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.sink.Sink;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@AllArgsConstructor
public class AsyncConsumer implements KafkaConsumer {
    private Sink sink;
    private GenericConsumer genericConsumer;
    private ExecutorService executorService;

    public void process() throws FilterException {
        while(true) {
            List<Message> messages = genericConsumer.readMessages();
            Future<List<Message>> submitFuture = executorService.submit(getCallable(messages));

        }
    }

    private void processSingleFetch(List<Message> messages) {
        final Set<TopicPartition> topicPartitions = allRecords.partitions();
        for (TopicPartition partition : topicPartitions) {
            final List partitionRecords = allRecords.records(partition);
            final Callable task = createTask(partitionRecords);
            final Future future = executorService.submit(task);
            final long lastOffset = getLastOffset(partitionRecords);
            addToFutureMap(partition, future, lastOffset);
            offsetMgmtService.addToSubmittedOffsets(partition, lastOffset);
        }
    }

    private Callable<List<Message>> createTask(List<Message> messages) {
        return () -> sink.pushMessage(messages);
    }


    public Callable<List<Message>> getCallable(List<Message> messages) {
        return () -> sink.pushMessage(messages);
    }
}