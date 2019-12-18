package com.gojek.esb.sink.http.client;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.http.client.deserializer.Deserializer;
import com.gojek.esb.util.Clock;
import com.newrelic.api.agent.Trace;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

import static com.gojek.esb.metrics.Metrics.HTTP_FIREHOSE_LATENCY;

@AllArgsConstructor
public class BasicHttpSinkClient implements HttpSinkClient {
    private String requestUrl;
    private Header header;
    private Deserializer deserializer;


    @Getter
    private final HttpClient httpClient;

    @Getter
    private final Clock clock;

    @Getter
    private final StatsDReporter statsDReporter;

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicHttpSinkClient.class.getName());

    public Logger getLogger() {
        return LOGGER;
    }

    @Trace(dispatcher = true)
    public HttpResponse executeBatch(List<EsbMessage> messages) throws DeserializerException {
        HttpPut batchPutMethod = createBatchPutMethod(messages);
        messages.forEach(message -> {
            statsDReporter.captureDurationSince(HTTP_FIREHOSE_LATENCY, Instant.ofEpochMilli(message.getTimestamp()));
        });
        return sendRequest(batchPutMethod);
    }

    private HttpPut createBatchPutMethod(List<EsbMessage> messages) throws DeserializerException {
        List<String> deserializedMessages = deserializer.deserialize(messages);
        HttpPut request = new HttpPut(requestUrl);
        header.getAll().forEach(request::addHeader);
        String content = deserializedMessages.toString();
        request.setEntity(new StringEntity(content, ContentType.APPLICATION_JSON));

        LOGGER.debug("Request URL: {}", requestUrl);
        LOGGER.debug("Request headers: {}", header.getAll());
        LOGGER.debug("Request content: {}", content);

        return request;
    }
}
