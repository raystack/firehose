package com.gojek.esb.sink.http.request;

import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.header.BasicHeader;
import com.gojek.esb.sink.http.request.uri.BasicUri;
import com.gojek.esb.sink.http.request.uri.UriParser;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DynamicUrlRequest implements Request {
    private BasicUri uri;
    private BasicHeader header;
    private JsonBody body;
    private HttpRequestMethod method;
    private UriParser uriParser;

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicUrlRequest.class);

    public DynamicUrlRequest(BasicUri uri, BasicHeader header, JsonBody body, HttpRequestMethod method, UriParser uriParser) {
        this.uri = uri;
        this.header = header;
        this.body = body;
        this.method = method;
        this.uriParser = uriParser;
    }

    public List<HttpEntityEnclosingRequestBase> build(List<EsbMessage> esbMessages) throws DeserializerException, URISyntaxException {
        List<HttpEntityEnclosingRequestBase> requests = new ArrayList<>();
        List<String> bodyContents = body.serialize(esbMessages);
        for (int i = 0; i < esbMessages.size(); i++) {
            EsbMessage esbMessage = esbMessages.get(i);
            URI requestUrl = uri.build(esbMessage, uriParser);
            HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory.create(requestUrl, method);

            header.build().forEach(request::addHeader);
            request.setEntity(buildHttpEntity(bodyContents.get(i)));
            requests.add(request);

            LOGGER.debug("Request URL: {}", requestUrl);
            LOGGER.debug("Request headers: {}", header.build());
            LOGGER.debug("Request content: {}", bodyContents.get(i));
            LOGGER.debug("Request method: {}", method);
        }
        return requests;
    }

    private StringEntity buildHttpEntity(String bodyContent) throws DeserializerException {
        String arrayWrappedBody = Collections.singletonList(bodyContent).toString();
        return new StringEntity(arrayWrappedBody, ContentType.APPLICATION_JSON);
    }

}
