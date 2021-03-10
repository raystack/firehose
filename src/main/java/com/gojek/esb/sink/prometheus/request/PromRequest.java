package com.gojek.esb.sink.prometheus.request;

import com.gojek.esb.consumer.Message;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.UriBuilder;
import com.gojek.esb.sink.prometheus.builder.RequestEntityBuilder;
import com.gojek.esb.sink.prometheus.builder.WriteRequestBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

public class PromRequest {
    private WriteRequestBuilder body;
    private StatsDReporter statsDReporter;
    private RequestEntityBuilder requestEntityBuilder;
    private PromRequestCreator requestCreator;


    public PromRequest(StatsDReporter statsDReporter, WriteRequestBuilder body) {
        this.body = body;
        this.statsDReporter = statsDReporter;
    }

    public HttpEntityEnclosingRequestBase build(List<Message> messages) throws DeserializerException, URISyntaxException, IOException {
        return requestCreator.create(messages, requestEntityBuilder);
    }

    public PromRequest setRequest(HeaderBuilder headerBuilder, UriBuilder uriBuilder, RequestEntityBuilder requestEntitybuilder) {
        this.requestCreator = new PromRequestCreator(new Instrumentation(statsDReporter, PromRequestCreator.class), uriBuilder, headerBuilder, body);
        this.requestEntityBuilder = requestEntitybuilder;
        return this;
    }
}
