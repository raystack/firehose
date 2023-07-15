package org.raystack.firehose.sink.http.request;


import org.raystack.firehose.config.HttpSinkConfig;
import org.raystack.firehose.config.enums.HttpSinkRequestMethodType;
import org.raystack.firehose.metrics.FirehoseInstrumentation;
import org.raystack.firehose.proto.ProtoToFieldMapper;
import org.raystack.firehose.serializer.MessageSerializer;
import org.raystack.firehose.sink.http.request.entity.RequestEntityBuilder;
import org.raystack.firehose.sink.http.request.header.HeaderBuilder;
import org.raystack.firehose.sink.http.request.types.ParameterizedHeaderRequest;
import org.raystack.firehose.sink.http.request.types.ParameterizedUriRequest;
import org.raystack.firehose.sink.http.request.types.Request;
import org.raystack.firehose.sink.http.request.types.SimpleRequest;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.firehose.sink.http.factory.SerializerFactory;
import org.raystack.firehose.sink.http.request.body.JsonBody;
import org.raystack.firehose.sink.http.request.types.DynamicUrlRequest;
import org.raystack.firehose.sink.http.request.uri.UriBuilder;
import org.raystack.firehose.sink.http.request.uri.UriParser;
import org.raystack.stencil.client.StencilClient;
import org.raystack.stencil.Parser;

import java.util.Arrays;
import java.util.List;

/**
 * Request factory create requests based on configuration.
 */
public class RequestFactory {

    private HttpSinkConfig httpSinkConfig;
    private UriParser uriParser;
    private StencilClient stencilClient;
    private StatsDReporter statsDReporter;
    private FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Instantiates a new Request factory.
     *
     * @param statsDReporter the statsd reporter
     * @param httpSinkConfig the http sink config
     * @param stencilClient  the stencil client
     * @param uriParser      the uri parser
     */
    public RequestFactory(StatsDReporter statsDReporter, HttpSinkConfig httpSinkConfig, StencilClient stencilClient, UriParser uriParser) {
        this.statsDReporter = statsDReporter;
        this.stencilClient = stencilClient;
        this.httpSinkConfig = httpSinkConfig;
        this.uriParser = uriParser;
        firehoseInstrumentation = new FirehoseInstrumentation(this.statsDReporter, RequestFactory.class);
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
        firehoseInstrumentation.logInfo("Request type: {}", request.getClass());

        return request.setRequestStrategy(headerBuilder, uriBuilder, requestEntityBuilder);
    }

    private ProtoToFieldMapper getProtoToFieldMapper() {
        Parser protoParser = stencilClient.getParser(httpSinkConfig.getSinkHttpParameterSchemaProtoClass());
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
