package com.gojek.esb.sink.http.request.types;

import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.create.BatchRequestCreator;
import com.gojek.esb.sink.http.request.create.IndividualRequestCreator;
import com.gojek.esb.sink.http.request.create.RequestCreator;
import com.gojek.esb.sink.http.request.entity.EntityBuilder;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.URIBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

public class BatchRequest implements Request {

    private HTTPSinkConfig httpSinkConfig;
    private JsonBody body;
    private HttpRequestMethod method;
    private EntityBuilder entityBuilder;
    private RequestCreator requestCreator;

    // TODO : Its just not batching anymore. Think of a better name
    public BatchRequest(HTTPSinkConfig config, JsonBody body, HttpRequestMethod method) {
        this.httpSinkConfig = config;
        this.body = body;
        this.method = method;
    }

    public List<HttpEntityEnclosingRequestBase> build(List<EsbMessage> esbMessages) throws DeserializerException, URISyntaxException {
        return requestCreator.create(esbMessages, entityBuilder);
    }

    // TODO : Forced to change the parameter field name entityBuilder due to checkstyle
    @Override
    public Request setRequestStrategy(HeaderBuilder headerBuilder, URIBuilder uriBuilder, EntityBuilder entitybuilder) {
        if (isTemplateBody(httpSinkConfig)) {
            this.requestCreator = new IndividualRequestCreator(uriBuilder, headerBuilder, method, body);
        } else {
            this.requestCreator = new BatchRequestCreator(uriBuilder, headerBuilder, method, body);
        }
        this.entityBuilder = entitybuilder;
        return this;
    }

    @Override
    public boolean canProcess() {
        boolean isDynamicUrl = httpSinkConfig.getServiceURL().contains(",");
        return httpSinkConfig.getHttpSinkParameterSource() == HttpSinkParameterSourceType.DISABLED && !isDynamicUrl;
    }
}
