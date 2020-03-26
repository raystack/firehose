package com.gojek.esb.sink.http.request;

import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.header.BasicHeader;
import com.gojek.esb.sink.http.request.uri.BasicUri;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

public class BatchRequest implements Request {
    private BasicUri uri;
    private BasicHeader header;
    private JsonBody body;
    private HttpRequestMethod method;

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchRequest.class);

    public BatchRequest(BasicUri uri, BasicHeader header, JsonBody body, HttpRequestMethod method) {
        this.uri = uri;
        this.header = header;
        this.body = body;
        this.method = method;
    }

    public List<HttpEntityEnclosingRequestBase> build(List<EsbMessage> esbMessages) throws DeserializerException, URISyntaxException {
        HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory.create(uri.build(), method);

        header.build().forEach(request::addHeader);
        request.setEntity(buildHttpEntity(esbMessages));

        LOGGER.debug("Request URL: {}", uri.build());
        LOGGER.debug("Request headers: {}", header.build());
        LOGGER.debug("Request content: {}", body.serialize(esbMessages));
        LOGGER.debug("Request method: {}", method);

        return Collections.singletonList(request);
    }

    private StringEntity buildHttpEntity(List<EsbMessage> esbMessages) throws DeserializerException {
        return new StringEntity(body.serialize(esbMessages).toString(), ContentType.APPLICATION_JSON);
    }

}
