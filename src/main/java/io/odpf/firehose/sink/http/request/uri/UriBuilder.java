package io.odpf.firehose.sink.http.request.uri;

import io.odpf.firehose.config.enums.HttpSinkParameterSourceType;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.proto.ProtoToFieldMapper;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Builds URI based on the requirement.
 */
public class UriBuilder {

    private String baseURL;
    private UriParser uriParser;
    private ProtoToFieldMapper protoToFieldMapper;
    private HttpSinkParameterSourceType httpSinkParameterSourceType;

    public UriBuilder(String baseURL, UriParser uriParser) {
        this.baseURL = baseURL;
        this.uriParser = uriParser;
    }

    public URI build() throws URISyntaxException {
        return new URI(baseURL);
    }

    public URI build(Message message) throws URISyntaxException {
        String url = uriParser.parse(message, baseURL);
        org.apache.http.client.utils.URIBuilder uriBuilder = new org.apache.http.client.utils.URIBuilder(url);
        if (protoToFieldMapper == null) {
            return uriBuilder.build();
        }

        // flow for parameterized URI
        Map<String, Object> paramMap = protoToFieldMapper
                .getFields((httpSinkParameterSourceType == HttpSinkParameterSourceType.KEY) ? message.getLogKey()
                        : message.getLogMessage());
        paramMap.forEach((string, object) -> uriBuilder.addParameter(string, object.toString()));
        return uriBuilder.build();
    }

    public UriBuilder withParameterizedURI(ProtoToFieldMapper protoToFieldmapper, HttpSinkParameterSourceType httpSinkParameterSource) {
        this.protoToFieldMapper = protoToFieldmapper;
        this.httpSinkParameterSourceType = httpSinkParameterSource;
        return this;
    }
}
