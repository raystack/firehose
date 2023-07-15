package org.raystack.firehose.sink.http.request.create;

import org.raystack.firehose.config.HttpSinkConfig;
import org.raystack.firehose.config.enums.HttpSinkRequestMethodType;
import org.raystack.firehose.message.Message;
import org.raystack.firehose.metrics.FirehoseInstrumentation;
import org.raystack.firehose.sink.http.request.entity.RequestEntityBuilder;
import org.raystack.firehose.sink.http.request.header.HeaderBuilder;
import org.raystack.firehose.sink.http.request.HttpRequestMethodFactory;
import org.raystack.firehose.sink.http.request.body.JsonBody;
import org.raystack.firehose.sink.http.request.uri.UriBuilder;
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
    private HttpSinkConfig httpSinkConfig;

    public BatchRequestCreator(FirehoseInstrumentation firehoseInstrumentation, UriBuilder uriBuilder, HeaderBuilder headerBuilder, HttpSinkRequestMethodType method, JsonBody jsonBody, HttpSinkConfig httpSinkConfig) {
        this.uriBuilder = uriBuilder;
        this.headerBuilder = headerBuilder;
        this.method = method;
        this.jsonBody = jsonBody;
        this.httpSinkConfig = httpSinkConfig;
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

        if (!(method == HttpSinkRequestMethodType.DELETE && !httpSinkConfig.getSinkHttpDeleteBodyEnable())) {
            request.setEntity(requestEntityBuilder.buildHttpEntity(messagesString));
            firehoseInstrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                    uri, headerMap, jsonBody.serialize(messages), method);
        } else {
            firehoseInstrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: no body\nRequest method: {}",
                    uri, headerMap, method);
        }
        return Collections.singletonList(request);
    }
}
