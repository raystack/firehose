package com.gojek.esb.sink.http.request.uri;

import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.proto.ProtoToFieldMapper;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Builds URI based on the requirement.
 */
public class URIBuilder {

    private String baseURL;
    private UriParser uriParser;
    private ProtoToFieldMapper protoToFieldMapper;
    private HttpSinkParameterSourceType httpSinkParameterSourceType;

    public URIBuilder(String baseURL, UriParser uriParser) {
        this.baseURL = baseURL;
        this.uriParser = uriParser;
    }

    public URI build() throws URISyntaxException {
        return new URI(baseURL);
    }

    public URI build(EsbMessage esbMessage) throws URISyntaxException {
        String url = uriParser.parse(esbMessage, baseURL);
        org.apache.http.client.utils.URIBuilder uriBuilder = new org.apache.http.client.utils.URIBuilder(url);
        if (protoToFieldMapper == null) {
            return uriBuilder.build();
        }

        // flow for parameterized URI
        Map<String, Object> paramMap = protoToFieldMapper
                .getFields((httpSinkParameterSourceType == HttpSinkParameterSourceType.KEY) ? esbMessage.getLogKey()
                        : esbMessage.getLogMessage());
        paramMap.forEach((string, object) -> uriBuilder.addParameter(string, object.toString()));
        return uriBuilder.build();
    }

    public URIBuilder withParameterizedURI(ProtoToFieldMapper protoToFieldmapper, HttpSinkParameterSourceType httpSinkParameterSource) {
        this.protoToFieldMapper = protoToFieldmapper;
        this.httpSinkParameterSourceType = httpSinkParameterSource;
        return this;
    }
}
