package com.gojek.esb.sink.http.request.create;

import com.gojek.esb.config.enums.HttpSinkRequestMethodType;
import com.gojek.esb.consumer.Message;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.http.request.HttpRequestMethodFactory;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.entity.RequestEntityBuilder;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.UriBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BatchRequestCreator implements RequestCreator {

    private UriBuilder uriBuilder;
    private HeaderBuilder headerBuilder;
    private HttpSinkRequestMethodType method;
    private JsonBody jsonBody;
    private Instrumentation instrumentation;

    public BatchRequestCreator(Instrumentation instrumentation, UriBuilder uriBuilder, HeaderBuilder headerBuilder, HttpSinkRequestMethodType method, JsonBody jsonBody) {
        this.uriBuilder = uriBuilder;
        this.headerBuilder = headerBuilder;
        this.method = method;
        this.jsonBody = jsonBody;
        this.instrumentation = instrumentation;
    }

    @Override
    public List<HttpEntityEnclosingRequestBase> create(List<Message> messages, RequestEntityBuilder requestEntityBuilder) throws URISyntaxException {
        URI uri = uriBuilder.build();
        HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory
                .create(uri, method);

        Map<String, String> headerMap = headerBuilder.build();
        headerMap.forEach(request::addHeader);
        String esbMessagesString = jsonBody.serialize(messages).toString();

        request.setEntity(requestEntityBuilder.buildHttpEntity(esbMessagesString));
        instrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uri, headerMap, jsonBody.serialize(messages), method);
        return Collections.singletonList(request);
    }
}
