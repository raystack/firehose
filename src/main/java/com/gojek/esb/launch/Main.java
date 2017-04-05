package com.gojek.esb.launch;

import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.exception.FilterException;
import com.gojek.esb.factory.LogConsumerFactory;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, DeserializerException, FilterException {
        LogConsumer logConsumer = new LogConsumerFactory(System.getenv()).buildConsumer();
        while (true) {
            logConsumer.processPartitions();
        }
    }
}
