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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gojek.esb.metrics.Metrics.HTTP_RESPONSE_CODE;
import static com.gojek.esb.metrics.Metrics.MESSAGES_DROPPED_COUNT;


/**
 * HttpSink implement {@link AbstractSink} lifecycle for HTTP.
 */
public class HttpSink extends AbstractSink {

    private static final int SUCCESS_CODE = 200;

    private Request request;
    private List<HttpEntityEnclosingRequestBase> httpRequests;
    private HttpClient httpClient;
    private StencilClient stencilClient;
    private Map<Integer, Boolean> retryStatusCodeRanges;
    private Map<Integer, Boolean> requestLogStatusCodeRanges;

    public HttpSink(Instrumentation instrumentation, Request request, HttpClient httpClient, StencilClient stencilClient, Map<Integer, Boolean> retryStatusCodeRanges, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        super(instrumentation, "http");
        this.request = request;
        this.httpClient = httpClient;
        this.stencilClient = stencilClient;
        this.retryStatusCodeRanges = retryStatusCodeRanges;
        this.requestLogStatusCodeRanges = requestLogStatusCodeRanges;
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
                if (shouldLogRequest(response)) {
                    printRequest(httpRequest);
                }
                if (shouldRetry(response)) {
                    throw new NeedToRetry(statusCode(response));
                } else if (response.getStatusLine().getStatusCode() != SUCCESS_CODE) {
                    captureMessageDropCount(response, httpRequest);
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

    private boolean shouldLogRequest(HttpResponse response) {
        return response == null || requestLogStatusCodeRanges.containsKey(response.getStatusLine().getStatusCode());
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
            httpCodeTag = "status_code=" + response.getStatusLine().getStatusCode();
        }
        getInstrumentation().captureCountWithTags(HTTP_RESPONSE_CODE, 1, httpCodeTag, urlTag);
    }

    private void printRequest(HttpEntityEnclosingRequestBase httpRequest) throws IOException {
        InputStream inputStream = getContent(httpRequest);
        String entireRequest = String.format("\nRequest Method: %s\nRequest Url: %s\nRequest Headers: %s\nRequest Body: %s",
                httpRequest.getMethod(),
                httpRequest.getURI(),
                Arrays.asList(httpRequest.getAllHeaders()),
                new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)).lines().collect(Collectors.joining("\n")));
        getInstrumentation().logInfo(entireRequest);
        inputStream.reset();
    }

    private InputStream getContent(HttpEntityEnclosingRequestBase httpRequest) throws IOException {
        return httpRequest.getEntity().getContent();
    }

    private void captureMessageDropCount(HttpResponse response, HttpEntityEnclosingRequestBase httpRequest) throws IOException {
        InputStream inputStream = getContent(httpRequest);
        String requestBody = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)).lines().collect(Collectors.joining("\n"));

        List<String> result = Arrays.asList(requestBody.replaceAll("^\\[|]$", "").split("},\\s*\\{"));

        getInstrumentation().captureCountWithTags(MESSAGES_DROPPED_COUNT, result.size(), "cause= " + statusCode(response));
        getInstrumentation().logInfo("Message dropped because of status code: " + statusCode(response));
    }
}
