package com.gojek.esb.sink.http.request.create;

import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.http.request.HttpRequestMethodFactory;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.entity.RequestEntityBuilder;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.URIBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BatchRequestCreator implements RequestCreator {

    private URIBuilder uriBuilder;
    private HeaderBuilder headerBuilder;
    private HttpRequestMethod method;
    private JsonBody jsonBody;
    private Instrumentation instrumentation;

    public BatchRequestCreator(Instrumentation instrumentation, URIBuilder uriBuilder, HeaderBuilder headerBuilder, HttpRequestMethod method, JsonBody jsonBody) {
        this.uriBuilder = uriBuilder;
        this.headerBuilder = headerBuilder;
        this.method = method;
        this.jsonBody = jsonBody;
        this.instrumentation = instrumentation;
    }

    @Override
    public List<HttpEntityEnclosingRequestBase> create(List<EsbMessage> esbMessages, RequestEntityBuilder requestEntityBuilder) throws URISyntaxException {
        URI uri = uriBuilder.build();
        HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory
                .create(uri, method);

        Map<String, String> headerMap = headerBuilder.build();
        headerMap.forEach(request::addHeader);
        String esbMessagesString = jsonBody.serialize(esbMessages).toString();

        request.setEntity(requestEntityBuilder.buildHttpEntity(esbMessagesString));
        instrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uri, headerMap, jsonBody.serialize(esbMessages), method);
        return Collections.singletonList(request);
    }
}
