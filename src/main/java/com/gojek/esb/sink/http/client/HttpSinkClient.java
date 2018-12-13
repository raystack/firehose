package com.gojek.esb.sink.http.client;

import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.util.Clock;
import com.newrelic.api.agent.NewRelic;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Instant;

import static com.gojek.esb.metrics.Metrics.HTTP_EXECUTION_TIME;
import static com.gojek.esb.metrics.Metrics.HTTP_RESPONSE_CODE;

public interface HttpSinkClient {
    HttpClient getHttpClient();
    Clock getClock();
    StatsDReporter getStatsDReporter();
    Logger getLogger();

    default HttpResponse sendRequest(HttpEntityEnclosingRequestBase method) {
        HttpResponse response = null;
        Instant beforeCall = getClock().now();
        try {
            response = getHttpClient().execute(method);
            getLogger().info("Response Status: " + status(response));
            return response;
        } catch (IOException e) {
            getLogger().error("Error while calling http sink service url", e);
            NewRelic.noticeError(e);
        } finally {
            consumeResponse(response);
            getStatsDReporter().captureDurationSince(HTTP_EXECUTION_TIME, beforeCall);
            String urlTag = "url=" + method.getURI().getPath();
            String httpCodeTag = "status_code=";
            if (response != null) {
                httpCodeTag = "status_code=" + Integer.toString(status(response));
            }
            getStatsDReporter().captureCount(HTTP_RESPONSE_CODE, 1, httpCodeTag, urlTag);
        }
        return response;
    }

    default void consumeResponse(HttpResponse response) {
        if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }

    default int status(HttpResponse response) {
        return response.getStatusLine().getStatusCode();
    }
}
