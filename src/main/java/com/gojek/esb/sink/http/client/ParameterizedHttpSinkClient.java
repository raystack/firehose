package com.gojek.esb.sink.http.client;

import com.gojek.esb.config.enums.HttpSinkParameterPlacementType;
import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.sink.http.client.deserializer.Deserializer;
import com.gojek.esb.util.Clock;
import com.google.common.base.CaseFormat;
import com.newrelic.api.agent.Trace;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gojek.esb.metrics.Metrics.HTTP_FIREHOSE_LATENCY;

@AllArgsConstructor
public class ParameterizedHttpSinkClient implements HttpSinkClient {
    private String requestUrl;
    private Header header;
    private Deserializer deserializer;
    private ProtoToFieldMapper protoToFieldMapper;
    private HttpSinkParameterSourceType httpSinkParameterSource;
    private HttpSinkParameterPlacementType httpSinkParameterPlacement;

    @Getter
    private final HttpClient httpClient;

    @Getter
    private final Clock clock;

    @Getter
    private final StatsDReporter statsDReporter;

    private static final Logger LOGGER = LoggerFactory.getLogger(ParameterizedHttpSinkClient.class.getName());

    public Logger getLogger() {
        return LOGGER;
    }

    @Trace(dispatcher = true)
    public HttpResponse execute(EsbMessage message) throws DeserializerException, URISyntaxException {

        HttpPut putMethod = createPutMethod(message);
        statsDReporter.captureDurationSince(HTTP_FIREHOSE_LATENCY, Instant.ofEpochMilli(message.getTimestamp()));
        return sendRequest(putMethod);
    }

    private HttpPut createPutMethod(EsbMessage message) throws DeserializerException, URISyntaxException {
        Map<String, Object> paramMap = protoToFieldMapper.getFields((httpSinkParameterSource == HttpSinkParameterSourceType.KEY) ? message.getLogKey() : message.getLogMessage());
        List<String> deserializedMessage = deserializer.deserialize(Collections.singletonList(message));

        URIBuilder uriBuilder = new URIBuilder(requestUrl);
        if (httpSinkParameterPlacement == HttpSinkParameterPlacementType.QUERY) {
            paramMap.forEach((string, object) -> uriBuilder.addParameter(string, object.toString()));
        }

        HttpPut request = new HttpPut(uriBuilder.build());
        if (httpSinkParameterPlacement == HttpSinkParameterPlacementType.HEADER) {
            Map<String, Object> paramMapWithCustomHeaders = paramMap.entrySet().stream().collect(Collectors.toMap(e -> convertToCustomHeaders(e.getKey()), e -> e.getValue()));
            paramMapWithCustomHeaders.forEach((string, object) -> request.addHeader(string, object.toString()));
        }
        header.getAll().forEach(request::addHeader);
        String content = deserializedMessage.toString();
        request.setEntity(new StringEntity(content, ContentType.APPLICATION_JSON));

        LOGGER.debug("Request URL: {}", requestUrl);
        LOGGER.debug("Request headers: {}", header.getAll());
        LOGGER.debug("Request content: {}", content);

        return request;
    }

    private String convertToCustomHeaders(String parameter) {
        String customHeader = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, parameter);
        customHeader = "X-" + customHeader;
        return customHeader;
    }

}
