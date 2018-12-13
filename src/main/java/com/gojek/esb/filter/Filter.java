package com.gojek.esb.filter;

import com.gojek.esb.consumer.EsbMessage;

import java.util.List;

/**
 * Interface for filtering the messages.
 */
public interface Filter {

    /**
     * The method used for filtering the messages.
     *
     * @param messages the protobuf records in binary format that are wrapped in {@link EsbMessage}
     * @return filtered messages.
     * @throws EsbFilterException
     */
    List<EsbMessage> filter(List<EsbMessage> messages) throws EsbFilterException;
}
