package com.gojek.esb.sink.http.request.create;

import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.sink.http.request.HttpRequestMethodFactory;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.entity.EntityBuilder;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.URIBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

public class BatchRequestCreator implements RequestCreator {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchRequestCreator.class);

    private URIBuilder uriBuilder;
    private HeaderBuilder headerBuilder;
    private HttpRequestMethod method;
    private JsonBody jsonBody;

    public BatchRequestCreator(URIBuilder uriBuilder, HeaderBuilder headerBuilder, HttpRequestMethod method, JsonBody jsonBody) {
        this.uriBuilder = uriBuilder;
        this.headerBuilder = headerBuilder;
        this.method = method;
        this.jsonBody = jsonBody;
    }

    @Override
    public List<HttpEntityEnclosingRequestBase> create(List<EsbMessage> esbMessages, EntityBuilder entityBuilder) throws URISyntaxException {
        HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory
                .create(uriBuilder.build(), method);

        headerBuilder.build().forEach(request::addHeader);
        String esbMessagesString = jsonBody.serialize(esbMessages).toString();
        request.setEntity(entityBuilder.buildHttpEntity(esbMessagesString));

        LOGGER.debug("Request URL: {}", uriBuilder.build());
        LOGGER.debug("Request headers: {}", headerBuilder.build());
        LOGGER.debug("Request content: {}", jsonBody.serialize(esbMessages));
        LOGGER.debug("Request method: {}", method);

        return Collections.singletonList(request);
    }
}
