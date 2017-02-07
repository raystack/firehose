package com.gojek.esb.launch;

import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.consumer.StreamingClient;
import com.gojek.esb.factory.FactoryUtils;
import com.gojek.esb.factory.LogConsumerFactory;
import com.gojek.esb.factory.StreamingClientFactory;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {

        LogConsumer logConsumer = new LogConsumerFactory(System.getenv()).getConsumer();
        //TODO:move initialize call to log consumer
        logConsumer.getSink().initialize();
        while (true) {
            logConsumer.processPartitions();
        }
    }
}
