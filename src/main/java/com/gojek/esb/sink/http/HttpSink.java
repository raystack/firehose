package com.gojek.esb.sink.http;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.http.client.BasicHttpSinkClient;
import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HTTPSink allows messages consumed from kafka to be relayed to a http service.
 * The related configurations for HTTPSink can be found here: {@see com.gojek.esb.config.ParameterizedHTTPSinkConfig}
 */
public class HttpSink extends BaseHttpSink {

    private BasicHttpSinkClient client;
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSink.class.getName());

    public HttpSink(BasicHttpSinkClient client, Map<Integer, Boolean> retryStatusCodeRanges) {
        super(retryStatusCodeRanges);
        this.client = client;
    }

    @Override
    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessages) throws DeserializerException {
        LOGGER.info("pushing {} messages", esbMessages.size());
        List<EsbMessage> failedMessages = new ArrayList<>();

        HttpResponse response = client.executeBatch(esbMessages);
        if (callUnsuccessful(response)) {
            failedMessages = esbMessages;

        }
        return failedMessages;
    }
}
