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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IndividualRequestCreator implements RequestCreator {

    private HeaderBuilder headerBuilder;
    private JsonBody jsonBody;
    private HttpSinkRequestMethodType method;
    private UriBuilder uriBuilder;
    private Instrumentation instrumentation;

    public IndividualRequestCreator(Instrumentation instrumentation, UriBuilder uriBuilder, HeaderBuilder headerBuilder, HttpSinkRequestMethodType method, JsonBody body) {
        this.uriBuilder = uriBuilder;
        this.headerBuilder = headerBuilder;
        this.jsonBody = body;
        this.method = method;
        this.instrumentation = instrumentation;
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
            request.setEntity(entity.buildHttpEntity(bodyContents.get(i)));

            instrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                    requestUrl, headerMap, bodyContents.get(i), method);

            requests.add(request);
        }
        return requests;
    }
}
