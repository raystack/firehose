package com.gojek.esb.sink.prometheus.request;

import com.gojek.esb.consumer.Message;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.UriBuilder;
import com.gojek.esb.sink.prometheus.builder.RequestEntityBuilder;
import com.gojek.esb.sink.prometheus.builder.WriteRequestBuilder;
import cortexpb.Cortex;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

public class PromRequestCreator {

    private HeaderBuilder headerBuilder;
    private UriBuilder uriBuilder;
    private Instrumentation instrumentation;
    private WriteRequestBuilder writeRequestBuilder;

    public PromRequestCreator(Instrumentation instrumentation, UriBuilder uriBuilder, HeaderBuilder headerBuilder, WriteRequestBuilder writeRequestBuilder) {
        this.uriBuilder = uriBuilder;
        this.headerBuilder = headerBuilder;
        this.writeRequestBuilder = writeRequestBuilder;
        this.instrumentation = instrumentation;
    }

    public HttpEntityEnclosingRequestBase create(List<Message> messages, RequestEntityBuilder entity) throws URISyntaxException, IOException {
        Cortex.WriteRequest writeRequest = writeRequestBuilder.buildWriteRequest(messages);
        URI uri = uriBuilder.build();
        HttpEntityEnclosingRequestBase request = new HttpPost(uri);
        Map<String, String> headerMap = headerBuilder.build();
        headerMap.forEach(request::addHeader);
        request.setEntity(entity.buildHttpEntity(writeRequest));
        instrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}",
                uri, headerMap, writeRequest.toString());
        return request;
    }
}
