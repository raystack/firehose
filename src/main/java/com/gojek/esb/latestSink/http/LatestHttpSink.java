package com.gojek.esb.latestSink.http;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.latestSink.AbstractSink;
import com.gojek.esb.latestSink.http.client.Header;
import com.gojek.esb.latestSink.http.client.deserializer.Deserializer;
import com.gojek.esb.metrics.Instrumentation;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LatestHttpSink extends AbstractSink {
    private Deserializer deserializer;
    private String requestUrl;
    private Header header;
    private HttpPut batchPutMethod;
    private final HttpClient httpClient;
    private final StencilClient stencilClient;
    private HttpResponse response;

    public LatestHttpSink(Instrumentation instrumentation, String sinkType, Deserializer deserializer, String requestUrl, Header header, HttpClient httpClient, StencilClient stencilClient) {
        super(instrumentation, sinkType);
        this.deserializer = deserializer;
        this.requestUrl = requestUrl;
        this.header = header;
        this.httpClient = httpClient;
        this.stencilClient = stencilClient;
    }

    @Override
    protected void prepare(List<EsbMessage> esbMessages) throws DeserializerException {
        batchPutMethod = createBatchPutMethod(esbMessages);
    }

    @Override
    @Trace(dispatcher = true)
    protected List<EsbMessage> execute() throws Exception {
        try {
            response = httpClient.execute(batchPutMethod);
            instrumentation.logInfo("Response Status: {}", response.getStatusLine().getStatusCode());
        } catch (IOException e) {
            instrumentation.captureFatalError(e, "Error while calling http sink service url");
            NewRelic.noticeError(e);
            throw e;
        } finally {
            instrumentation.captureHttpStatusCount(batchPutMethod, response);
            consumeResponse(response);
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        stencilClient.close();
    }

    private HttpPut createBatchPutMethod(List<EsbMessage> messages) throws DeserializerException {
        List<String> deserializedMessages = deserializer.deserialize(messages);
        HttpPut request = new HttpPut(requestUrl);
        header.getAll().forEach(request::addHeader);
        String content = deserializedMessages.toString();
        request.setEntity(new StringEntity(content, ContentType.APPLICATION_JSON));

        instrumentation.logDebug("Request URL: {}", requestUrl);
        instrumentation.logDebug("Request headers: {}", header.getAll());
        instrumentation.logDebug("Request content: {}", content);

        return request;
    }

    private void consumeResponse(HttpResponse response) {
        if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }
}
