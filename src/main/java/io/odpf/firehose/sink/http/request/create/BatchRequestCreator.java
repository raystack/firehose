package io.odpf.firehose.sink.http.request.create;

import io.odpf.firehose.config.enums.HttpSinkRequestMethodType;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.sink.http.request.HttpRequestMethodFactory;
import io.odpf.firehose.sink.http.request.body.JsonBody;
import io.odpf.firehose.sink.http.request.entity.RequestEntityBuilder;
import io.odpf.firehose.sink.http.request.header.HeaderBuilder;
import io.odpf.firehose.sink.http.request.uri.UriBuilder;
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
    private FirehoseInstrumentation firehoseInstrumentation;

    public BatchRequestCreator(FirehoseInstrumentation firehoseInstrumentation, UriBuilder uriBuilder, HeaderBuilder headerBuilder, HttpSinkRequestMethodType method, JsonBody jsonBody) {
        this.uriBuilder = uriBuilder;
        this.headerBuilder = headerBuilder;
        this.method = method;
        this.jsonBody = jsonBody;
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    @Override
    public List<HttpEntityEnclosingRequestBase> create(List<Message> messages, RequestEntityBuilder requestEntityBuilder) throws URISyntaxException {
        URI uri = uriBuilder.build();
        HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory
                .create(uri, method);

        Map<String, String> headerMap = headerBuilder.build();
        headerMap.forEach(request::addHeader);
        String messagesString = jsonBody.serialize(messages).toString();

        request.setEntity(requestEntityBuilder.buildHttpEntity(messagesString));
        firehoseInstrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                uri, headerMap, jsonBody.serialize(messages), method);
        return Collections.singletonList(request);
    }
}
