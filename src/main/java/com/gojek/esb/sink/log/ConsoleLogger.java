package com.gojek.esb.sink.log;

import com.google.protobuf.DynamicMessage;

/**
 * class to log the esb proto to console.
 */
public class ConsoleLogger implements ProtoLogger {

    @Override
    public void log(DynamicMessage key, DynamicMessage message) {
        System.out.println("===================KEY==========================");
        System.out.println(key);
        System.out.println("================= MESSAGE=======================");
        System.out.println(message);
    }
}
