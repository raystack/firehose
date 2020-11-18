package com.gojek.esb.sink.http.request.types;

import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.create.BatchRequestCreator;
import com.gojek.esb.sink.http.request.create.IndividualRequestCreator;
import com.gojek.esb.sink.http.request.create.RequestCreator;
import com.gojek.esb.sink.http.request.entity.RequestEntityBuilder;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.URIBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

public class SimpleRequest implements Request {

    private HTTPSinkConfig httpSinkConfig;
    private JsonBody body;
    private HttpRequestMethod method;
    private RequestEntityBuilder requestEntityBuilder;
    private RequestCreator requestCreator;
    private StatsDReporter statsDReporter;

    public SimpleRequest(StatsDReporter statsDReporter, HTTPSinkConfig config, JsonBody body, HttpRequestMethod method) {
        this.httpSinkConfig = config;
        this.body = body;
        this.method = method;
        this.statsDReporter = statsDReporter;
    }

    public List<HttpEntityEnclosingRequestBase> build(List<EsbMessage> esbMessages) throws DeserializerException, URISyntaxException {
        return requestCreator.create(esbMessages, requestEntityBuilder);
    }

    @Override
    public Request setRequestStrategy(HeaderBuilder headerBuilder, URIBuilder uriBuilder, RequestEntityBuilder requestEntitybuilder) {
        if (isTemplateBody(httpSinkConfig)) {
            this.requestCreator = new IndividualRequestCreator(new Instrumentation(
                    statsDReporter, IndividualRequestCreator.class), uriBuilder, headerBuilder, method, body);
        } else {
            this.requestCreator = new BatchRequestCreator(new Instrumentation(
                    statsDReporter, BatchRequestCreator.class), uriBuilder, headerBuilder, method, body);
        }
        this.requestEntityBuilder = requestEntitybuilder;
        return this;
    }

    @Override
    public boolean canProcess() {
        boolean isDynamicUrl = httpSinkConfig.getServiceURL().contains(",");
        return httpSinkConfig.getHttpSinkParameterSource() == HttpSinkParameterSourceType.DISABLED && !isDynamicUrl;
    }
}
