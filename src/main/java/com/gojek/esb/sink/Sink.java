package com.gojek.esb.sink;

import com.gojek.esb.consumer.Message;
import com.gojek.esb.exception.DeserializerException;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * An interface for developing custom Sinks to FirehoseConsumer.
 */
public interface Sink extends Closeable {

    /**
     * method to write batch of messages read from kafka.
     * The logic of how to persist the data goes in here.
     *
     * @param message list of {@see EsbMessage}
     * @throws IOException           in case of error conditions while persisting it to the custom sink.
     * @throws DeserializerException in case of problems with deserialising the message into a protobuf object.
     */
    List<Message> pushMessage(List<Message> message) throws IOException, DeserializerException;

}
