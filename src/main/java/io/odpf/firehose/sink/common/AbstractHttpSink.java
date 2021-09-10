package io.odpf.firehose.sink.common;


import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.NeedToRetry;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import com.gojek.de.stencil.client.StencilClient;
import joptsimple.internal.Strings;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.odpf.firehose.metrics.Metrics.SINK_HTTP_RESPONSE_CODE_TOTAL;

public abstract class AbstractHttpSink extends AbstractSink {

    private final List<HttpEntityEnclosingRequestBase> httpRequests = new ArrayList<>();
    private final HttpClient httpClient;
    private final StencilClient stencilClient;
    private final Map<Integer, Boolean> retryStatusCodeRanges;
    private final Map<Integer, Boolean> requestLogStatusCodeRanges;
    protected static final String SUCCESS_CODE_PATTERN = "^2.*";

    public AbstractHttpSink(Instrumentation instrumentation, String sinkType, HttpClient httpClient, StencilClient stencilClient, Map<Integer, Boolean> retryStatusCodeRanges, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        super(instrumentation, sinkType);
        this.httpClient = httpClient;
        this.stencilClient = stencilClient;
        this.retryStatusCodeRanges = retryStatusCodeRanges;
        this.requestLogStatusCodeRanges = requestLogStatusCodeRanges;
    }

    @Override
    @Trace(dispatcher = true)
    public List<Message> execute() throws Exception {
        HttpResponse response = null;
        for (HttpEntityEnclosingRequestBase httpRequest : httpRequests) {
            try {
                response = httpClient.execute(httpRequest);
                List<String> contentStringList = null;
                getInstrumentation().logInfo("Response Status: {}", statusCode(response));
                if (shouldLogResponse(response)) {
                    printResponse(response);
                }
                if (shouldLogRequest(response)) {
                    contentStringList = readContent(httpRequest);
                    printRequest(httpRequest, contentStringList);
                }
                if (shouldRetry(response)) {
                    throw new NeedToRetry(statusCode(response));
                } else if (!Pattern.compile(SUCCESS_CODE_PATTERN).matcher(String.valueOf(response.getStatusLine().getStatusCode())).matches()) {
                    contentStringList = contentStringList == null ? readContent(httpRequest) : contentStringList;
                    captureMessageDropCount(response, contentStringList);
                }
            } catch (IOException e) {
                NewRelic.noticeError(e);
                throw e;
            } finally {
                consumeResponse(response);
                captureHttpStatusCount(httpRequest, response);
            }
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        getInstrumentation().logInfo("HTTP connection closing");
        getHttpRequests().clear();
        getStencilClient().close();
    }


    private void consumeResponse(HttpResponse response) {
        if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }

    private boolean shouldLogRequest(HttpResponse response) {
        return response == null || getRequestLogStatusCodeRanges().containsKey(response.getStatusLine().getStatusCode());
    }

    private boolean shouldLogResponse(HttpResponse response) {
        return getInstrumentation().isDebugEnabled() && response != null;
    }

    private boolean shouldRetry(HttpResponse response) {
        return response == null || getRetryStatusCodeRanges().containsKey(response.getStatusLine().getStatusCode());
    }

    protected String statusCode(HttpResponse response) {
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
        getInstrumentation().captureCountWithTags(SINK_HTTP_RESPONSE_CODE_TOTAL, 1, httpCodeTag, urlTag);
    }


    private void printRequest(HttpEntityEnclosingRequestBase httpRequest, List<String> contentStringList) throws IOException {
        String entireRequest = String.format("\nRequest Method: %s\nRequest Url: %s\nRequest Headers: %s\nRequest Body: %s",
                httpRequest.getMethod(),
                httpRequest.getURI(),
                Arrays.asList(httpRequest.getAllHeaders()),
                Strings.join(contentStringList, "\n"));
        getInstrumentation().logInfo(entireRequest);
    }

    private void printResponse(HttpResponse httpResponse) throws IOException {
        try (InputStream inputStream = httpResponse.getEntity().getContent()) {
            String responseBody = String.format("Response Body: %s",
                    Strings.join(new BufferedReader(new InputStreamReader(
                            inputStream,
                            StandardCharsets.UTF_8)).lines().collect(Collectors.toList()), "\n"));
            getInstrumentation().logDebug(responseBody);
        }
    }

    protected abstract List<String> readContent(HttpEntityEnclosingRequestBase httpRequest) throws IOException;

    protected abstract void captureMessageDropCount(HttpResponse response, List<String> contentString) throws IOException;

    public void setHttpRequests(List<HttpEntityEnclosingRequestBase> httpRequests) {
        this.httpRequests.clear();
        this.httpRequests.addAll(httpRequests);
    }

    public List<HttpEntityEnclosingRequestBase> getHttpRequests() {
        return httpRequests;
    }

    public StencilClient getStencilClient() {
        return stencilClient;
    }

    public Map<Integer, Boolean> getRetryStatusCodeRanges() {
        return retryStatusCodeRanges;
    }

    public Map<Integer, Boolean> getRequestLogStatusCodeRanges() {
        return requestLogStatusCodeRanges;
    }
}
