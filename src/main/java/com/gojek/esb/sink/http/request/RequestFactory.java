package com.gojek.esb.sink.http.request;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.serializer.EsbMessageSerializer;
import com.gojek.esb.sink.http.factory.SerializerFactory;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.entity.RequestEntityBuilder;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.types.DynamicUrlRequest;
import com.gojek.esb.sink.http.request.types.ParameterizedHeaderRequest;
import com.gojek.esb.sink.http.request.types.ParameterizedURIRequest;
import com.gojek.esb.sink.http.request.types.Request;
import com.gojek.esb.sink.http.request.types.SimpleRequest;
import com.gojek.esb.sink.http.request.uri.URIBuilder;
import com.gojek.esb.sink.http.request.uri.UriParser;

import java.util.Arrays;
import java.util.List;

public class RequestFactory {

    private HTTPSinkConfig httpSinkConfig;
    private UriParser uriParser;
    private StencilClient stencilClient;
    private StatsDReporter statsDReporter;
    private Instrumentation instrumentation;

    public RequestFactory(StatsDReporter statsDReporter, HTTPSinkConfig httpSinkConfig, StencilClient stencilClient, UriParser uriParser) {
        this.statsDReporter = statsDReporter;
        this.stencilClient = stencilClient;
        this.httpSinkConfig = httpSinkConfig;
        this.uriParser = uriParser;
        instrumentation = new Instrumentation(this.statsDReporter, RequestFactory.class);
    }

    public Request createRequest() {
        JsonBody body = createBody();
        HttpRequestMethod httpRequestMethod = httpSinkConfig.getHttpSinkRequestMethod();
        HeaderBuilder headerBuilder = new HeaderBuilder(httpSinkConfig.getHTTPHeaders());
        URIBuilder uriBuilder = new URIBuilder(httpSinkConfig.getServiceURL(), uriParser);
        RequestEntityBuilder requestEntityBuilder = new RequestEntityBuilder();

        List<Request> requests = Arrays.asList(
                new SimpleRequest(statsDReporter, httpSinkConfig, body, httpRequestMethod),
                new DynamicUrlRequest(statsDReporter, httpSinkConfig, body, httpRequestMethod),
                new ParameterizedHeaderRequest(statsDReporter, httpSinkConfig, body, httpRequestMethod, getProtoToFieldMapper()),
                new ParameterizedURIRequest(statsDReporter, httpSinkConfig, body, httpRequestMethod, getProtoToFieldMapper()));

        Request request = requests.stream()
                .filter(Request::canProcess)
                .findFirst()
                .orElse(new SimpleRequest(statsDReporter, httpSinkConfig, body, httpRequestMethod));
        instrumentation.logInfo("Request type: {}", request.getClass());

        return request.setRequestStrategy(headerBuilder, uriBuilder, requestEntityBuilder);
    }

    private ProtoToFieldMapper getProtoToFieldMapper() {
        ProtoParser protoParser = new ProtoParser(stencilClient, httpSinkConfig.getParameterProtoSchema());
        return new ProtoToFieldMapper(protoParser, httpSinkConfig.getProtoToFieldMapping());
    }

    private JsonBody createBody() {
        EsbMessageSerializer esbMessageSerializer = new SerializerFactory(
                httpSinkConfig,
                stencilClient,
                statsDReporter)
                .build();
        return new JsonBody(esbMessageSerializer);
    }
}
