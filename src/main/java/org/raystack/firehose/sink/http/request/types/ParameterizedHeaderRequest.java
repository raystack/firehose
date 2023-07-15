package org.raystack.firehose.sink.http.request.types;

import org.raystack.firehose.config.HttpSinkConfig;
import org.raystack.firehose.config.enums.HttpSinkParameterPlacementType;
import org.raystack.firehose.config.enums.HttpSinkParameterSourceType;
import org.raystack.firehose.config.enums.HttpSinkRequestMethodType;
import org.raystack.firehose.exception.DeserializerException;
import org.raystack.firehose.message.Message;
import org.raystack.firehose.metrics.FirehoseInstrumentation;
import org.raystack.firehose.proto.ProtoToFieldMapper;
import org.raystack.firehose.sink.http.request.header.HeaderBuilder;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.firehose.sink.http.request.body.JsonBody;
import org.raystack.firehose.sink.http.request.create.IndividualRequestCreator;
import org.raystack.firehose.sink.http.request.create.RequestCreator;
import org.raystack.firehose.sink.http.request.entity.RequestEntityBuilder;
import org.raystack.firehose.sink.http.request.uri.UriBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

/**
 * ParameterizedRequest create one HttpPut per-message. Uri and Header are
 * parametrized according to incoming message.
 */
public class ParameterizedHeaderRequest implements Request {

    private StatsDReporter statsDReporter;
    private HttpSinkConfig httpSinkConfig;
    private JsonBody body;
    private HttpSinkRequestMethodType method;
    private RequestEntityBuilder requestEntityBuilder;
    private ProtoToFieldMapper protoToFieldMapper;
    private RequestCreator requestCreator;

    /**
     * Instantiates a new Parameterized header request.
     *
     * @param statsDReporter     the stats d reporter
     * @param httpSinkConfig     the http sink config
     * @param body               the body
     * @param method             the method
     * @param protoToFieldMapper the proto to field mapper
     */
    public ParameterizedHeaderRequest(StatsDReporter statsDReporter,
                                      HttpSinkConfig httpSinkConfig,
                                      JsonBody body,
                                      HttpSinkRequestMethodType method,
                                      ProtoToFieldMapper protoToFieldMapper) {

        this.statsDReporter = statsDReporter;
        this.httpSinkConfig = httpSinkConfig;
        this.body = body;
        this.method = method;
        this.protoToFieldMapper = protoToFieldMapper;
    }

    public List<HttpEntityEnclosingRequestBase> build(List<Message> messages) throws URISyntaxException, DeserializerException {
        return requestCreator.create(messages, requestEntityBuilder.setWrapping(!isTemplateBody(httpSinkConfig)));
    }

    /**
     * Sets request strategy.
     *
     * @param headerBuilder        the header builder
     * @param uriBuilder           the uri builder
     * @param requestEntitybuilder the request entitybuilder
     * @return the request strategy
     */
    @Override
    public Request setRequestStrategy(HeaderBuilder headerBuilder, UriBuilder uriBuilder, RequestEntityBuilder requestEntitybuilder) {
        this.requestCreator = new IndividualRequestCreator(
                new FirehoseInstrumentation(statsDReporter, IndividualRequestCreator.class), uriBuilder,
                headerBuilder.withParameterizedHeader(protoToFieldMapper, httpSinkConfig.getSinkHttpParameterSource()),
                method, body, httpSinkConfig);
        this.requestEntityBuilder = requestEntitybuilder;
        return this;
    }

    @Override
    public boolean canProcess() {
        return httpSinkConfig.getSinkHttpParameterSource() != HttpSinkParameterSourceType.DISABLED
                && httpSinkConfig.getSinkHttpParameterPlacement() == HttpSinkParameterPlacementType.HEADER;
    }
}
