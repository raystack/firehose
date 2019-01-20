package com.gojek.esb.sink.http;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.http.client.ParameterizedHttpSinkClient;
import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ParameterizedHttpSink allows messages consumed from kafka to be relayed to a http service where it deserialize and append to headers or
 * query string based on the config.
 */
public class ParameterizedHttpSink extends BaseHttpSink {

    private ParameterizedHttpSinkClient client;
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSink.class.getName());

    public ParameterizedHttpSink(ParameterizedHttpSinkClient client, Map<Integer, Boolean> retryStatusCodeRanges, StencilClient stencilClient) {
        super(retryStatusCodeRanges, stencilClient);
        this.client = client;
    }

    @Override
    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessages) throws IOException, DeserializerException {

        LOGGER.info("pushing {} messages", esbMessages.size());
        List<EsbMessage> failedMessages = new ArrayList<>();

        for (EsbMessage esbMessage : esbMessages) {
            HttpResponse response;
            try {
                response = client.execute(esbMessage);
            } catch (URISyntaxException e) {
                throw new IOException("Incorrect URI syntax");
            }
            if (callUnsuccessful(response)) {
                failedMessages.add(esbMessage);
            }
        }

        return failedMessages;
    }

}
