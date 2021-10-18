package io.odpf.firehose.consumer.common;

import io.odpf.firehose.filter.FilterException;

import java.io.Closeable;
import java.io.IOException;

public interface FirehoseConsumer extends Closeable {

    void process() throws IOException, FilterException;
}
