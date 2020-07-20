package com.gojek.esb.sink.http;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.exception.NeedToRetry;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.http.request.types.Request;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.gojek.esb.metrics.Metrics.HTTP_RESPONSE_CODE;

/**
 * HttpSink implement {@link AbstractSink} lifecycle for HTTP.
 */
public class HttpSink extends AbstractSink {

    private Request request;
    private List<HttpEntityEnclosingRequestBase> httpRequests;
    private HttpClient httpClient;
    private StencilClient stencilClient;
    private Map<Integer, Boolean> retryStatusCodeRanges;

    public HttpSink(Instrumentation instrumentation, Request request, HttpClient httpClient, StencilClient stencilClient, Map<Integer, Boolean> retryStatusCodeRanges) {
        super(instrumentation, "http");
        this.request = request;
        this.httpClient = httpClient;
        this.stencilClient = stencilClient;
        this.retryStatusCodeRanges = retryStatusCodeRanges;
    }

    @Override
    protected void prepare(List<EsbMessage> esbMessages) throws DeserializerException, IOException {
        try {
            httpRequests = request.build(esbMessages);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    @Trace(dispatcher = true)
    protected List<EsbMessage> execute() throws Exception {
        HttpResponse response = null;
        for (HttpEntityEnclosingRequestBase httpRequest : httpRequests) {
            try {
                response = httpClient.execute(httpRequest);
                getInstrumentation().logInfo("Response Status: {}", statusCode(response));
                if (shouldRetry(response)) {
                    throw new NeedToRetry(statusCode(response));
                }
            } catch (IOException e) {
                NewRelic.noticeError(e);
                throw e;
            } finally {
                consumeResponse(response);
                captureHttpStatusCount(httpRequest, response);
                response = null;
            }
        }
        return new ArrayList<>();
    }


    @Override
    public void close() throws IOException {
        this.httpRequests = new ArrayList<>();
        stencilClient.close();
    }

    private void consumeResponse(HttpResponse response) {
        if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }

    private boolean shouldRetry(HttpResponse response) {
        return response == null || retryStatusCodeRanges.containsKey(response.getStatusLine().getStatusCode());
    }

    private String statusCode(HttpResponse response) {
        if (response != null) {
            return Integer.toString(response.getStatusLine().getStatusCode());
        } else {
            return "null";
        }
    }

    private void captureHttpStatusCount(HttpEntityEnclosingRequestBase httpRequestMethod, HttpResponse response) {
        String urlTag = "url=" + httpRequestMethod.getURI().getPath();
        String httpCodeTag = "status_code=";
        if (response != null) {
            httpCodeTag = "status_code=" + Integer.toString(response.getStatusLine().getStatusCode());
        }
        getInstrumentation().captureCountWithTags(HTTP_RESPONSE_CODE, 1, httpCodeTag, urlTag);
    }
}
