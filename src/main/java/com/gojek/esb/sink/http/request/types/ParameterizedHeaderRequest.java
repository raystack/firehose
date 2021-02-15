package com.gojek.esb.sink.http.request.types;

import com.gojek.esb.config.HttpSinkConfig;
import com.gojek.esb.config.enums.HttpSinkRequestMethodType;
import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.consumer.Message;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.create.IndividualRequestCreator;
import com.gojek.esb.sink.http.request.create.RequestCreator;
import com.gojek.esb.sink.http.request.entity.RequestEntityBuilder;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.UriBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

import static com.gojek.esb.config.enums.HttpSinkParameterPlacementType.HEADER;

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

    @Override
    public Request setRequestStrategy(HeaderBuilder headerBuilder, UriBuilder uriBuilder, RequestEntityBuilder requestEntitybuilder) {
        this.requestCreator = new IndividualRequestCreator(
                new Instrumentation(statsDReporter, IndividualRequestCreator.class), uriBuilder,
                headerBuilder.withParameterizedHeader(protoToFieldMapper, httpSinkConfig.getSinkHttpParameterSource()),
                method, body);
        this.requestEntityBuilder = requestEntitybuilder;
        return this;
    }

    @Override
    public boolean canProcess() {
        return httpSinkConfig.getSinkHttpParameterSource() != HttpSinkParameterSourceType.DISABLED
                && httpSinkConfig.getSinkHttpParameterPlacement() == HEADER;
    }
}
