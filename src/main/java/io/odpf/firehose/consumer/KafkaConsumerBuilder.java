
package io.odpf.firehose.consumer;

import io.odpf.firehose.sink.Sink;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

enum KafkaConsumerMode {
    ASYNC,
    SYNC
}

public class KafkaConsumerBuilder {
    private Sink sink;
    private GenericConsumer genericConsumer;
    private ExecutorService executorService;
    private KafkaConsumerMode mode;

    public KafkaConsumerBuilder withSink(Sink sink)  {
        this.sink = sink;
        return this;
    }

    public KafkaConsumerBuilder withConsumer(GenericConsumer genericConsumer) {
        this.genericConsumer = genericConsumer;
        return this;
    }

    public KafkaConsumerBuilder withParallelism(int numProcesses) {
        this.executorService = Executors.newFixedThreadPool(numProcesses);
        return this;
    }

    public KafkaConsumerBuilder mode(KafkaConsumerMode mode) {
        this.mode = mode;
        return this;
    }

    public KafkaConsumer build() {
        if(mode == KafkaConsumerMode.ASYNC) {
            return new AsyncConsumer(sink, genericConsumer, executorService);
        }
    }
}