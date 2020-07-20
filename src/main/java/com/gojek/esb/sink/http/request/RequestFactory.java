package com.gojek.esb.sink.http.request;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.serializer.EsbMessageSerializer;
import com.gojek.esb.sink.http.factory.SerializerFactory;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.entity.EntityBuilder;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.types.BatchRequest;
import com.gojek.esb.sink.http.request.types.DynamicUrlRequest;
import com.gojek.esb.sink.http.request.types.ParameterizedHeaderRequest;
import com.gojek.esb.sink.http.request.types.ParameterizedURIRequest;
import com.gojek.esb.sink.http.request.types.Request;
import com.gojek.esb.sink.http.request.uri.URIBuilder;
import com.gojek.esb.sink.http.request.uri.UriParser;

import java.util.Arrays;
import java.util.List;

public class RequestFactory {

    private HTTPSinkConfig httpSinkConfig;
    private UriParser uriParser;
    private StencilClient stencilClient;

    public RequestFactory(HTTPSinkConfig httpSinkConfig, StencilClient stencilClient, UriParser uriParser) {
        this.stencilClient = stencilClient;
        this.httpSinkConfig = httpSinkConfig;
        this.uriParser = uriParser;
    }

    public Request createRequest() {
        JsonBody body = createBody();
        HttpRequestMethod httpRequestMethod = httpSinkConfig.getHttpSinkRequestMethod();
        HeaderBuilder headerBuilder = new HeaderBuilder(httpSinkConfig.getHTTPHeaders());
        URIBuilder uriBuilder = new URIBuilder(httpSinkConfig.getServiceURL(), uriParser);
        EntityBuilder entityBuilder = new EntityBuilder();

        List<Request> requests = Arrays.asList(
                new BatchRequest(httpSinkConfig, body, httpRequestMethod),
                new DynamicUrlRequest(httpSinkConfig, body, httpRequestMethod),
                new ParameterizedHeaderRequest(httpSinkConfig, body, httpRequestMethod, getProtoToFieldMapper()),
                new ParameterizedURIRequest(httpSinkConfig, body, httpRequestMethod, getProtoToFieldMapper()));

        return requests.stream()
                .filter(Request::canProcess)
                .findFirst()
                .orElse(new BatchRequest(httpSinkConfig, body, httpRequestMethod))
                .setRequestStrategy(headerBuilder, uriBuilder, entityBuilder);
    }

    private ProtoToFieldMapper getProtoToFieldMapper() {
        ProtoParser protoParser = new ProtoParser(stencilClient, httpSinkConfig.getParameterProtoSchema());
        return new ProtoToFieldMapper(protoParser, httpSinkConfig.getProtoToFieldMapping());
    }

    private JsonBody createBody() {
        EsbMessageSerializer esbMessageSerializer = new SerializerFactory(
                httpSinkConfig,
                stencilClient).build();
        return new JsonBody(esbMessageSerializer);
    }
}
