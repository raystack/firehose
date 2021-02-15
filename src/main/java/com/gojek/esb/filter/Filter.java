package com.gojek.esb.filter;

import com.gojek.esb.consumer.Message;

import java.util.List;

/**
 * Interface for filtering the messages.
 */
public interface Filter {

    /**
     * The method used for filtering the messages.
     *
     * @param messages the protobuf records in binary format that are wrapped in {@link Message}
     * @return filtered messages.
     * @throws FilterException
     */
    List<Message> filter(List<Message> messages) throws FilterException;
}
