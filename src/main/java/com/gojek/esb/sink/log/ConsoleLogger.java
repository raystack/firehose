package com.gojek.esb.sink.log;

import com.google.protobuf.DynamicMessage;

/**
 * class to log the esb proto to console.
 */
public class ConsoleLogger implements ProtoLogger {

    @Override
    public void log(DynamicMessage message) {
        System.out.println("================= DATA =======================");
        System.out.println(message);
    }
}
