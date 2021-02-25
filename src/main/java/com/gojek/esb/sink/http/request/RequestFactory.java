package com.gojek.esb.sink.http.request;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.HttpSinkConfig;
import com.gojek.esb.config.enums.HttpSinkRequestMethodType;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.serializer.MessageSerializer;
import com.gojek.esb.sink.http.factory.SerializerFactory;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.entity.RequestEntityBuilder;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.types.DynamicUrlRequest;
import com.gojek.esb.sink.http.request.types.ParameterizedHeaderRequest;
import com.gojek.esb.sink.http.request.types.ParameterizedUriRequest;
import com.gojek.esb.sink.http.request.types.Request;
import com.gojek.esb.sink.http.request.types.SimpleRequest;
import com.gojek.esb.sink.http.request.uri.UriBuilder;
import com.gojek.esb.sink.http.request.uri.UriParser;

import java.util.Arrays;
import java.util.List;

public class RequestFactory {

    private HttpSinkConfig httpSinkConfig;
    private UriParser uriParser;
    private StencilClient stencilClient;
    private StatsDReporter statsDReporter;
    private Instrumentation instrumentation;

    public RequestFactory(StatsDReporter statsDReporter, HttpSinkConfig httpSinkConfig, StencilClient stencilClient, UriParser uriParser) {
        this.statsDReporter = statsDReporter;
        this.stencilClient = stencilClient;
        this.httpSinkConfig = httpSinkConfig;
        this.uriParser = uriParser;
        instrumentation = new Instrumentation(this.statsDReporter, RequestFactory.class);
    }

    public Request createRequest() {
        JsonBody body = createBody();
        HttpSinkRequestMethodType httpSinkRequestMethodType = httpSinkConfig.getSinkHttpRequestMethod();
        HeaderBuilder headerBuilder = new HeaderBuilder(httpSinkConfig.getSinkHttpHeaders());
        UriBuilder uriBuilder = new UriBuilder(httpSinkConfig.getSinkHttpServiceUrl(), uriParser);
        RequestEntityBuilder requestEntityBuilder = new RequestEntityBuilder();

        List<Request> requests = Arrays.asList(
                new SimpleRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType),
                new DynamicUrlRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType),
                new ParameterizedHeaderRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType, getProtoToFieldMapper()),
                new ParameterizedUriRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType, getProtoToFieldMapper()));

        Request request = requests.stream()
                .filter(Request::canProcess)
                .findFirst()
                .orElse(new SimpleRequest(statsDReporter, httpSinkConfig, body, httpSinkRequestMethodType));
        instrumentation.logInfo("Request type: {}", request.getClass());

        return request.setRequestStrategy(headerBuilder, uriBuilder, requestEntityBuilder);
    }

    private ProtoToFieldMapper getProtoToFieldMapper() {
        ProtoParser protoParser = new ProtoParser(stencilClient, httpSinkConfig.getSinkHttpParameterProtoSchema());
        return new ProtoToFieldMapper(protoParser, httpSinkConfig.getInputSchemaProtoToColumnMapping());
    }

    private JsonBody createBody() {
        MessageSerializer messageSerializer = new SerializerFactory(
                httpSinkConfig,
                stencilClient,
                statsDReporter)
                .build();
        return new JsonBody(messageSerializer);
    }
}
