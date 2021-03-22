package io.odpf.firehose.sink.prometheus.request;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.http.request.uri.UriBuilder;
import io.odpf.firehose.sink.prometheus.builder.HeaderBuilder;
import io.odpf.firehose.sink.prometheus.builder.RequestEntityBuilder;
import io.odpf.firehose.sink.prometheus.builder.WriteRequestBuilder;
import cortexpb.Cortex;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

public class PromRequest {
    private Instrumentation instrumentation;
    private WriteRequestBuilder writeRequestBuilder;
    private UriBuilder uriBuilder;
    private RequestEntityBuilder requestEntityBuilder;
    private HeaderBuilder headerBuilder;


    public PromRequest(Instrumentation instrumentation, HeaderBuilder headerBuilder, UriBuilder uriBuilder,
                       RequestEntityBuilder requestEntityBuilder, WriteRequestBuilder writeRequestBuilder) {
        this.instrumentation = instrumentation;
        this.writeRequestBuilder = writeRequestBuilder;
        this.headerBuilder = headerBuilder;
        this.uriBuilder = uriBuilder;
        this.requestEntityBuilder = requestEntityBuilder;
    }

    public HttpEntityEnclosingRequestBase build(List<Message> messages) throws DeserializerException, URISyntaxException, IOException {
        Cortex.WriteRequest writeRequest = writeRequestBuilder.buildWriteRequest(messages);
        URI uri = uriBuilder.build();
        HttpEntityEnclosingRequestBase request = new HttpPost(uri);
        Map<String, String> headerMap = headerBuilder.build();
        headerMap.forEach(request::addHeader);
        request.setEntity(requestEntityBuilder.buildHttpEntity(writeRequest));
        instrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}", uri, headerMap, writeRequest.toString());
        return request;
    }
}
