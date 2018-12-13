package com.gojek.esb.sink.clevertap;

import com.gojek.esb.config.ClevertapSinkConfig;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.http.client.Header;
import com.gojek.esb.sink.http.client.HttpSinkClient;
import com.gojek.esb.util.Clock;
import com.google.gson.GsonBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class Clevertap implements HttpSinkClient {

    private final String url;
    private final HttpClient client;
    private final Header headers;
    private final Clock clock;
    private final StatsDReporter statsDReporter;
    private static final String MYSTERIOUS_D_WRAPPER_JSON_TEMPLATE_FOR_PAYLOAD = "{d:%s}";
    private static final Logger LOGGER = LoggerFactory.getLogger(Clevertap.class);


    public Clevertap(ClevertapSinkConfig config, HttpClient client, Clock clock, StatsDReporter statsDReporter) {
        this.url = config.getServiceURL();
        this.headers = new Header(config.getHTTPHeaders());
        this.client = client;
        this.clock = clock;
        this.statsDReporter = statsDReporter;
    }

    @Override
    public HttpClient getHttpClient() {
        return client;
    }

    @Override
    public Clock getClock() {
        return clock;
    }

    @Override
    public StatsDReporter getStatsDReporter() {
        return statsDReporter;
    }

    @Override
    public Logger getLogger() {
        return LOGGER;
    }

    public HttpResponse sendEvents(List<ClevertapEvent> events) throws IOException {

        HttpPost request = new HttpPost(this.url);
        String eventPayload = new GsonBuilder().create().toJson(events);

        String payloadForClevertap = String.format(MYSTERIOUS_D_WRAPPER_JSON_TEMPLATE_FOR_PAYLOAD, eventPayload);
        LOGGER.debug(payloadForClevertap);

        request.setEntity(new StringEntity(payloadForClevertap, ContentType.APPLICATION_JSON));
        headers.getAll().forEach(request::addHeader);

        return sendRequest(request);
    }
}
