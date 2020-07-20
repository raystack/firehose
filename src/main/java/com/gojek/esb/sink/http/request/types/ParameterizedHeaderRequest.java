package com.gojek.esb.sink.http.request.types;

import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.create.IndividualRequestCreator;
import com.gojek.esb.sink.http.request.create.RequestCreator;
import com.gojek.esb.sink.http.request.entity.EntityBuilder;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.URIBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

import static com.gojek.esb.config.enums.HttpSinkParameterPlacementType.HEADER;

/**
 * ParameterizedRequest create one HttpPut per-message. Uri and Header are
 * parametrized according to incoming message.
 */
public class ParameterizedHeaderRequest implements Request {

    private HTTPSinkConfig httpSinkConfig;
    private JsonBody body;
    private HttpRequestMethod method;
    private EntityBuilder entityBuilder;
    private ProtoToFieldMapper protoToFieldMapper;
    private RequestCreator requestCreator;

    public ParameterizedHeaderRequest(HTTPSinkConfig httpSinkConfig,
                                      JsonBody body,
                                      HttpRequestMethod method,
                                      ProtoToFieldMapper protoToFieldMapper) {
        this.httpSinkConfig = httpSinkConfig;
        this.body = body;
        this.method = method;
        this.protoToFieldMapper = protoToFieldMapper;
    }

    public List<HttpEntityEnclosingRequestBase> build(List<EsbMessage> esbMessages) throws URISyntaxException, DeserializerException {
        return requestCreator.create(esbMessages, entityBuilder.setWrapping(!isTemplateBody(httpSinkConfig)));
    }

    @Override
    public Request setRequestStrategy(HeaderBuilder headerBuilder, URIBuilder uriBuilder, EntityBuilder entitybuilder) {
        this.requestCreator = new IndividualRequestCreator(uriBuilder,
                headerBuilder.withParameterizedHeader(protoToFieldMapper, httpSinkConfig.getHttpSinkParameterSource()), method, body);
        this.entityBuilder = entitybuilder;
        return this;
    }

    @Override
    public boolean canProcess() {
        return httpSinkConfig.getHttpSinkParameterSource() != HttpSinkParameterSourceType.DISABLED
                && httpSinkConfig.getHttpSinkParameterPlacement() == HEADER;
    }
}
