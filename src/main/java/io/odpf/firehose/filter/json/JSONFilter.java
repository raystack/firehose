package io.odpf.firehose.filter.json;

import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.filter.Filter;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.metrics.Instrumentation;

import java.util.List;

public class JSONFilter implements Filter {
    public JSONFilter(KafkaConsumerConfig consumerConfig, Instrumentation instrumentation) {
    }

    @Override
    public List<Message> filter(List<Message> messages) throws FilterException {
        return null;
    }
}
