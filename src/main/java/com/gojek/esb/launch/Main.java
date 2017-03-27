package com.gojek.esb.launch;

import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.factory.LogConsumerFactory;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, DeserializerException {
        LogConsumer logConsumer = new LogConsumerFactory(System.getenv()).buildConsumer();
        logConsumer.getSink();
        while (true) {
            logConsumer.processPartitions();
        }
    }
}
