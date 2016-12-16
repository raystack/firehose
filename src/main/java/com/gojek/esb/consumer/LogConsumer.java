package com.gojek.esb.consumer;

import com.gojek.esb.client.GenericHTTPClient;
import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class LogConsumer {

    private static final Logger logger = LoggerFactory.getLogger(LogConsumer.class);

    private EsbGenericConsumer consumer;
    private GenericHTTPClient genericHTTPClient;

    public LogConsumer(EsbGenericConsumer consumer, GenericHTTPClient genericHTTPClient) {
        this.consumer = consumer;
        this.genericHTTPClient = genericHTTPClient;
    }

    public void processPartitions() throws IOException {
        List<EsbMessage> messages = consumer.readMessages();
        if (!messages.isEmpty()) {
            HttpResponse resp = genericHTTPClient.execute(messages);
            logger.info("Execution successful for {} records", messages.size());

            consumer.commitAsync();
        }
    }
}
