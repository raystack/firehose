package com.gojek.esb.sink.log;

import com.google.protobuf.DynamicMessage;

/**
 * Interface to log the esb proto.
 */
public interface ProtoLogger {

    /**
     * method to log the protobuf (key and message found in proto definition in ESB).
     * @param key key proto message
     * @param message message part of the protobuf.
     */
    void log(DynamicMessage message);
}
