package com.gojek.esb.latestSink.http;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.config.enums.HttpSinkParameterPlacementType;
import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.latestSink.AbstractSink;
import com.gojek.esb.latestSink.http.client.Header;
import com.gojek.esb.latestSink.http.client.deserializer.Deserializer;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.google.common.base.CaseFormat;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LatestParameterizedHttpSink extends AbstractSink {
    private ProtoToFieldMapper protoToFieldMapper;
    private HttpSinkParameterSourceType httpSinkParameterSource;
    private Deserializer deserializer;
    private String requestUrl;
    private HttpSinkParameterPlacementType httpSinkParameterPlacement;
    private Header header;
    private List<HttpPut> httpPuts;
    private HttpClient httpClient;
    private StencilClient stencilClient;

    public LatestParameterizedHttpSink(Instrumentation instrumentation, String sinkType, ProtoToFieldMapper protoToFieldMapper, HttpSinkParameterSourceType httpSinkParameterSource, Deserializer deserializer, String requestUrl, HttpSinkParameterPlacementType httpSinkParameterPlacement, Header header, HttpClient httpClient, StencilClient stencilClient) {
        super(instrumentation, sinkType);
        this.protoToFieldMapper = protoToFieldMapper;
        this.httpSinkParameterSource = httpSinkParameterSource;
        this.deserializer = deserializer;
        this.requestUrl = requestUrl;
        this.httpSinkParameterPlacement = httpSinkParameterPlacement;
        this.header = header;
        this.httpClient = httpClient;
        this.stencilClient = stencilClient;
    }

    @Override
    protected void prepare(List<EsbMessage> esbMessages) throws DeserializerException, IOException {
        httpPuts = new ArrayList<>();
        for (EsbMessage esbMessage : esbMessages) {
            try {
                httpPuts.add(createPutMethod(esbMessage));
            } catch (URISyntaxException e) {
                throw new IOException("Incorrect URI syntax");
            }
        }

    }

    @Override
    @Trace(dispatcher = true)
    protected List<EsbMessage> execute() throws Exception {
        HttpResponse response = null;
        for (HttpPut httpPut : httpPuts) {
            try {
                response = httpClient.execute(httpPut);
                instrumentation.logInfo("Response Status: {}", response.getStatusLine().getStatusCode());
            } catch (IOException e) {
                instrumentation.captureFatalError(e, "Error while calling http sink service url");
                NewRelic.noticeError(e);
                throw e;
            } finally {
                consumeResponse(response);
                instrumentation.captureHttpStatusCount(httpPut, response);
                response = null;
            }
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        stencilClient.close();
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

        instrumentation.logDebug("Request URL: {}", requestUrl);
        instrumentation.logDebug("Request headers: {}", header.getAll());
        instrumentation.logDebug("Request content: {}", content);

        return request;
    }

    private String convertToCustomHeaders(String parameter) {
        String customHeader = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, parameter);
        customHeader = "X-" + customHeader;
        return customHeader;
    }

    private void consumeResponse(HttpResponse response) {
        if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }
}
