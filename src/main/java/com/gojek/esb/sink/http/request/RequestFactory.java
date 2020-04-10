package com.gojek.esb.sink.http.request;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.config.ParameterizedHTTPSinkConfig;
import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.config.enums.HttpSinkParameterPlacementType;
import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.serializer.EsbMessageSerializer;
import com.gojek.esb.sink.http.factory.SerializerFactory;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.header.BasicHeader;
import com.gojek.esb.sink.http.request.header.ParameterizedHeader;
import com.gojek.esb.sink.http.request.uri.BasicUri;
import com.gojek.esb.sink.http.request.uri.ParameterizedUri;
import com.gojek.esb.sink.http.request.uri.UriParser;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

import static com.gojek.esb.config.enums.HttpSinkParameterPlacementType.HEADER;

public class RequestFactory {

    private Map<String, String> configuration;
    private HTTPSinkConfig httpSinkConfig;
    private UriParser uriParser;
    private StencilClient stencilClient;

    public RequestFactory(Map<String, String> configuration, StencilClient stencilClient, UriParser uriParser) {
        this.configuration = configuration;
        this.stencilClient = stencilClient;
        httpSinkConfig = ConfigFactory.create(HTTPSinkConfig.class, configuration);
        this.uriParser = uriParser;
    }

    public Request create() {
        HttpRequestMethod httpRequestMethod = httpSinkConfig.getHttpSinkRequestMethod();
        if (httpSinkConfig.getHttpSinkParameterSource() == HttpSinkParameterSourceType.DISABLED) {
            BasicUri basicUri = new BasicUri(httpSinkConfig.getServiceURL());
            BasicHeader basicHeader = new BasicHeader(httpSinkConfig.getHTTPHeaders());
            if (uriParser.isDynamicUrl(httpSinkConfig.getServiceURL())) {
                return new DynamicUrlRequest(basicUri, basicHeader, createBody(), httpRequestMethod, uriParser);
            } else {
                return new BatchRequest(basicUri, basicHeader, createBody(), httpRequestMethod);
            }
        }


        ParameterizedHTTPSinkConfig parameterizedHttpSinkConfig = ConfigFactory.create(ParameterizedHTTPSinkConfig.class, configuration);

        ProtoParser protoParser = new ProtoParser(stencilClient, parameterizedHttpSinkConfig.getParameterProtoSchema());
        ProtoToFieldMapper protoToFieldMapper = new ProtoToFieldMapper(protoParser, parameterizedHttpSinkConfig.getProtoToFieldMapping());

        HttpSinkParameterPlacementType placementType = parameterizedHttpSinkConfig.getHttpSinkParameterPlacement();
        HttpSinkParameterSourceType parameterSource = parameterizedHttpSinkConfig.getHttpSinkParameterSource();
        String headers = parameterizedHttpSinkConfig.getHTTPHeaders();
        if (placementType == HEADER) {
            BasicUri basicUri = new BasicUri(httpSinkConfig.getServiceURL());
            ParameterizedHeader parameterizedHeader = new ParameterizedHeader(protoToFieldMapper, parameterSource, new BasicHeader(headers));
            return new ParameterizedRequest(basicUri, parameterizedHeader, createBody(), httpRequestMethod, uriParser);

        } else {
            ParameterizedUri parameterizedUri = new ParameterizedUri(httpSinkConfig.getServiceURL(), protoToFieldMapper, parameterSource);
            BasicHeader basicHeader = new BasicHeader(headers);
            return new ParameterizedRequest(parameterizedUri, basicHeader, createBody(), httpRequestMethod, uriParser);
        }
    }

    private JsonBody createBody() {
        EsbMessageSerializer esbMessageSerializer = new SerializerFactory(
                httpSinkConfig,
                stencilClient).build();
        return new JsonBody(esbMessageSerializer);
    }

}
