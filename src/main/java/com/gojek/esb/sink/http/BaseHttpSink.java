package com.gojek.esb.sink.http;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.Sink;
import org.apache.http.HttpResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * BaseHttpSink helps to abstract out the common HTTP sink functionality.
 */
public abstract class BaseHttpSink implements Sink {

    private Map<Integer, Boolean> retryStatusCodeRanges;

    public BaseHttpSink(Map<Integer, Boolean> retryStatusCodeRanges) {
        this.retryStatusCodeRanges = retryStatusCodeRanges;
    }

    public abstract List<EsbMessage> pushMessage(List<EsbMessage> esbMessages) throws IOException, DeserializerException;

    @Override
    public void close() throws IOException {
    }


    boolean callUnsuccessful(HttpResponse response) {
        return response == null || retryStatusCodeRanges.containsKey(status(response));
    }

    int status(HttpResponse response) {
        return response.getStatusLine().getStatusCode();
    }
}
