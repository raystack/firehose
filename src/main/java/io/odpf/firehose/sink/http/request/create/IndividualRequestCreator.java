package io.odpf.firehose.sink.http.request.create;

import io.odpf.firehose.config.HttpSinkConfig;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IndividualRequestCreator implements RequestCreator {

    private HeaderBuilder headerBuilder;
    private JsonBody jsonBody;
    private HttpSinkRequestMethodType method;
    private UriBuilder uriBuilder;
    private FirehoseInstrumentation firehoseInstrumentation;
    private HttpSinkConfig httpSinkConfig;

    public IndividualRequestCreator(FirehoseInstrumentation firehoseInstrumentation, UriBuilder uriBuilder, HeaderBuilder headerBuilder, HttpSinkRequestMethodType method, JsonBody body, HttpSinkConfig httpSinkConfig) {
        this.uriBuilder = uriBuilder;
        this.headerBuilder = headerBuilder;
        this.jsonBody = body;
        this.method = method;
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.httpSinkConfig = httpSinkConfig;
    }

    @Override
    public List<HttpEntityEnclosingRequestBase> create(List<Message> messages, RequestEntityBuilder entity) throws URISyntaxException {
        List<HttpEntityEnclosingRequestBase> requests = new ArrayList<>();
        List<String> bodyContents = jsonBody.serialize(messages);
        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            URI requestUrl = uriBuilder.build(message);
            HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory.create(requestUrl, method);

            Map<String, String> headerMap = headerBuilder.build(message);
            headerMap.forEach(request::addHeader);
            if (!(method == HttpSinkRequestMethodType.DELETE && !httpSinkConfig.getSinkHttpDeleteBodyEnable())) {
                request.setEntity(entity.buildHttpEntity(bodyContents.get(i)));

                firehoseInstrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                        requestUrl, headerMap, bodyContents.get(i), method);
            } else {
                firehoseInstrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: no body\nRequest method: {}",
                        requestUrl, headerMap, method);
            }
            requests.add(request);
        }
        return requests;
    }
}
