package org.raystack.firehose.sink.http;


import org.raystack.firehose.exception.DeserializerException;
import org.raystack.firehose.message.Message;
import org.raystack.firehose.metrics.FirehoseInstrumentation;
import org.raystack.firehose.metrics.Metrics;
import org.raystack.firehose.sink.common.AbstractHttpSink;
import org.raystack.firehose.sink.http.request.types.Request;
import org.raystack.stencil.client.StencilClient;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

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


/**
 * HttpSink implement {@link AbstractHttpSink } lifecycle for HTTP.
 */
public class HttpSink extends AbstractHttpSink {

    private final Request request;

    /**
     * Instantiates a new Http sink.
     *
     * @param firehoseInstrumentation    the instrumentation
     * @param request                    the request
     * @param httpClient                 the http client
     * @param stencilClient              the stencil client
     * @param retryStatusCodeRanges      the retry status code ranges
     * @param requestLogStatusCodeRanges the request log status code ranges
     */
    public HttpSink(FirehoseInstrumentation firehoseInstrumentation, Request request, HttpClient httpClient, StencilClient stencilClient, Map<Integer, Boolean> retryStatusCodeRanges, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        super(firehoseInstrumentation, "http", httpClient, stencilClient, retryStatusCodeRanges, requestLogStatusCodeRanges);
        this.request = request;
    }

    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException {
        try {
            setHttpRequests(request.build(messages));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected List<String> readContent(HttpEntityEnclosingRequestBase httpRequest) throws IOException {
        if (httpRequest.getMethod().equals("DELETE") && httpRequest.getEntity() == null) {
            return new ArrayList<>();
        }
        try (InputStream inputStream = httpRequest.getEntity().getContent()) {
            return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)).lines().collect(Collectors.toList());
        }
    }

    protected void captureMessageDropCount(HttpResponse response, List<String> contentStringList) {
        String requestBody = joptsimple.internal.Strings.join(contentStringList, "\n");

        List<String> result = Arrays.asList(requestBody.replaceAll("^\\[|]$", "").split("},\\s*\\{"));

        getFirehoseInstrumentation().captureCount(Metrics.SINK_MESSAGES_DROP_TOTAL, (long) result.size(), "cause= " + statusCode(response));
        getFirehoseInstrumentation().logInfo("Message dropped because of status code: " + statusCode(response));
    }
}
