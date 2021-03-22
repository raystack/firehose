package io.odpf.firehose.sink.prometheus;

import com.gojek.de.stencil.client.StencilClient;
import io.odpf.firehose.sink.prometheus.request.PromRequest;
import com.google.protobuf.DynamicMessage;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import cortexpb.Cortex;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.exception.NeedToRetry;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.util.EntityUtils;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static io.odpf.firehose.metrics.Metrics.SINK_HTTP_RESPONSE_CODE_TOTAL;
import static io.odpf.firehose.metrics.Metrics.SINK_MESSAGES_DROP_TOTAL;

public class PromSink extends AbstractSink {

    private static final String SUCCESS_CODE_PATTERN = "^2.*";

    private PromRequest request;
    private HttpEntityEnclosingRequestBase httpRequests;
    private HttpClient httpClient;
    private StencilClient stencilClient;
    private Map<Integer, Boolean> retryStatusCodeRanges;
    private Map<Integer, Boolean> requestLogStatusCodeRanges;


    public PromSink(Instrumentation instrumentation, PromRequest request, HttpClient httpClient, StencilClient stencilClient, Map<Integer, Boolean> retryStatusCodeRanges, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        super(instrumentation, "prometheus");
        this.request = request;
        this.httpClient = httpClient;
        this.stencilClient = stencilClient;
        this.retryStatusCodeRanges = retryStatusCodeRanges;
        this.requestLogStatusCodeRanges = requestLogStatusCodeRanges;
    }

    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException {
        try {
            httpRequests = request.build(messages);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    @Trace(dispatcher = true)
    protected List<Message> execute() throws Exception {
        HttpResponse response = null;
        try {
            response = httpClient.execute(httpRequests);
            getInstrumentation().logInfo("Response Status: {}", statusCode(response));
            if (shouldLogRequest(response)) {
                printRequest(httpRequests);
            }
            if (shouldRetry(response)) {
                throw new NeedToRetry(statusCode(response));
            } else if (!Pattern.compile(SUCCESS_CODE_PATTERN).matcher(String.valueOf(response.getStatusLine().getStatusCode())).matches()) {
                captureMessageDropCount(response, httpRequests);
            }
        } catch (IOException e) {
            NewRelic.noticeError(e);
            throw e;
        } finally {
            consumeResponse(response);
            captureHttpStatusCount(httpRequests, response);
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        getInstrumentation().logInfo("HTTP connection closing");
        this.httpRequests = null;
        stencilClient.close();
    }

    private void consumeResponse(HttpResponse response) throws IOException {
        if (response != null) {
            InputStream inputStream = getResponseContent(response);
            String entireResponse = String.format("\nResponse Code: %s\nResponse Headers: %s\nResponse Body: %s",
                    response.getStatusLine().getStatusCode(),
                    Arrays.toString(response.getAllHeaders()),
                    readContent(inputStream));
            getInstrumentation().logDebug(entireResponse);
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
        getInstrumentation().captureCountWithTags(SINK_HTTP_RESPONSE_CODE_TOTAL, 1, httpCodeTag, urlTag);
    }

    private void printRequest(HttpEntityEnclosingRequestBase httpRequest) throws IOException {
        InputStream inputStream = getRequestContent(httpRequest);
        String entireRequest = String.format("\nRequest Method: %s\nRequest Url: %s\nRequest Headers: %s\nRequest Body: %s",
                httpRequest.getMethod(),
                httpRequest.getURI(),
                Arrays.asList(httpRequest.getAllHeaders()),
                readContent(inputStream));
        getInstrumentation().logInfo(entireRequest);
        inputStream.reset();
    }

    private InputStream getRequestContent(HttpEntityEnclosingRequestBase httpRequest) throws IOException {
        return httpRequest.getEntity().getContent();
    }

    private InputStream getResponseContent(HttpResponse response) throws IOException {
        return response.getEntity().getContent();
    }

    private void captureMessageDropCount(HttpResponse response, HttpEntityEnclosingRequestBase httpRequest) throws IOException {
        InputStream inputStream = getRequestContent(httpRequest);
        List<String> result = readContent(inputStream);

        getInstrumentation().captureCountWithTags(SINK_MESSAGES_DROP_TOTAL, result.size(), "cause= " + statusCode(response));
        getInstrumentation().logInfo("Message dropped because of status code: " + statusCode(response));
    }

    private List<String> readContent(InputStream inputStream) throws IOException {
        byte[] byteArrayIs = IOUtils.toByteArray(inputStream);
        if (ArrayUtils.isEmpty(byteArrayIs)) {
            return new ArrayList<>();
        } else {
            byte[] uncompressedSnappy = Snappy.uncompress(byteArrayIs);
            String requestBody = DynamicMessage.parseFrom(Cortex.WriteRequest.getDescriptor(), uncompressedSnappy).toString();
            return Arrays.asList(requestBody.split("\n(?=timeseries)"));
        }

    }
}
