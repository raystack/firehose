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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IndividualRequestCreator implements RequestCreator {

    private HeaderBuilder headerBuilder;
    private JsonBody jsonBody;
    private HttpRequestMethod method;
    private URIBuilder uriBuilder;
    private Instrumentation instrumentation;

    public IndividualRequestCreator(Instrumentation instrumentation, URIBuilder uriBuilder, HeaderBuilder headerBuilder, HttpRequestMethod method, JsonBody body) {
        this.uriBuilder = uriBuilder;
        this.headerBuilder = headerBuilder;
        this.jsonBody = body;
        this.method = method;
        this.instrumentation = instrumentation;
    }

    @Override
    public List<HttpEntityEnclosingRequestBase> create(List<EsbMessage> esbMessages, RequestEntityBuilder entity) throws URISyntaxException {
        List<HttpEntityEnclosingRequestBase> requests = new ArrayList<>();
        List<String> bodyContents = jsonBody.serialize(esbMessages);
        for (int i = 0; i < esbMessages.size(); i++) {
            EsbMessage esbMessage = esbMessages.get(i);
            URI requestUrl = uriBuilder.build(esbMessage);
            HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory.create(requestUrl, method);

            Map<String, String> headerMap = headerBuilder.build(esbMessage);
            headerMap.forEach(request::addHeader);
            request.setEntity(entity.buildHttpEntity(bodyContents.get(i)));

            instrumentation.logDebug("\nRequest URL: {}\nRequest headers: {}\nRequest content: {}\nRequest method: {}",
                    requestUrl, headerMap, bodyContents.get(i), method);

            requests.add(request);
        }
        return requests;
    }
}
