package io.odpf.firehose.consumer;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.sink.Sink;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class FirehoseAsyncConsumer implements KafkaConsumer {
    private final Sink sink;
    private final GenericConsumer genericConsumer;
    private final ExecutorService executorService;
//    private final OffsetMgmtService offsetMgmtService;

    private Map<TopicPartition, List<Pair<Long, Future<List<Message>>>>> partitionFutureMap;

    public FirehoseAsyncConsumer(Sink sink, GenericConsumer genericConsumer, ExecutorService executorService) {
        this.sink = sink;
        this.genericConsumer = genericConsumer;
        this.executorService = executorService;
        partitionFutureMap = new HashMap<>();
    }

    public void process() throws FilterException {
        while (true) {
            ConsumerRecords<byte[], byte[]> allRecords = genericConsumer.getRecords();
            processSingleFetch(allRecords);
            handleAnyFinishedFutures();
        }
    }

    private void processSingleFetch(ConsumerRecords<byte[], byte[]> allRecords) throws FilterException {
        final Set<TopicPartition> topicPartitions = allRecords.partitions();
        for (TopicPartition partition : topicPartitions) {
            final List<ConsumerRecord<byte[], byte[]>> partitionRecords = allRecords.records(partition);
            final Callable<List<Message>> task = createTask(getFilteredMessages(partitionRecords));
            final Future<List<Message>> future = executorService.submit(task);
            final long lastOffset = getLastOffset(partitionRecords);
            addToFutureMap(partition, future, lastOffset);
//            offsetMgmtService.addToSubmittedOffsets(partition, lastOffset);
        }
    }

    private List<Message> getFilteredMessages(List<ConsumerRecord<byte[], byte[]>> records) throws FilterException {
        List<Message> messages = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            messages.add(new Message(record.key(), record.value(), record.topic(), record.partition(), record.offset(), record.headers(), record.timestamp(), System.currentTimeMillis()));
        }
        return genericConsumer.getFilter().filter(messages);
    }

    private void addToFutureMap(TopicPartition partition, Future<List<Message>> future, Long lastOffset) {
        final List<Pair<Long, Future<List<Message>>>> futureList = partitionFutureMap.getOrDefault(partition, new ArrayList<>());
        final Pair<Long, Future<List<Message>>> pair = new ImmutablePair<>(lastOffset, future);
        futureList.add(pair);
        partitionFutureMap.put(partition, futureList);
    }

    private long getLastOffset(List<ConsumerRecord<byte[], byte[]>> records) {
        return records.get(records.size() - 1).offset();
    }

    private Callable<List<Message>> createTask(List<Message> messages) {
        return () -> sink.pushMessage(messages);
    }

    private void handleAnyFinishedFutures() {
        final HashMap<TopicPartition, List<Long>> finishedBatches = new HashMap<>();
        for (TopicPartition partition : partitionFutureMap.keySet()) {
            final ArrayList unFinishedFutures = new ArrayList<Pair<Long, Future>>();
            for (Pair<Long, Future<List<Message>>> pair : partitionFutureMap.get(partition)) {
                if (pair.getValue().isDone()) {
                    final List<Long> finishedOffsets = finishedBatches.getOrDefault(partition, new ArrayList<>());
                    finishedOffsets.add(pair.getKey());
                    finishedBatches.put(partition, finishedOffsets);
                } else {
                    unFinishedFutures.add(pair);
                }
            }
            partitionFutureMap.put(partition, unFinishedFutures);
        }
//        offsetMgmtService.commitOffsets(finishedBatches);
    }

    @Override
    public void close() throws IOException {

    }
}
