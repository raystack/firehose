package com.gojek.esb.sink.http;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.Sink;
import lombok.AllArgsConstructor;
import org.apache.http.HttpResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * BaseHttpSink helps to abstract out the common HTTP sink functionality.
 */
@AllArgsConstructor
public abstract class BaseHttpSink implements Sink {

    private Map<Integer, Boolean> retryStatusCodeRanges;
    private StencilClient stencilClient;

    public abstract List<EsbMessage> pushMessage(List<EsbMessage> esbMessages) throws IOException, DeserializerException;

    @Override
    public void close() throws IOException {
        stencilClient.close();
    }


    boolean callUnsuccessful(HttpResponse response) {
        return response == null || retryStatusCodeRanges.containsKey(status(response));
    }

    private int status(HttpResponse response) {
        return response.getStatusLine().getStatusCode();
    }
}
