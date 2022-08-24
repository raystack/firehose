package io.odpf.firehose.sink.http.request;

import io.odpf.firehose.config.enums.HttpSinkRequestMethodType;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;

import java.net.URI;

/**
 * The type Http request method factory.
 */
public class HttpRequestMethodFactory {
    /**
     * Create http entity enclosing request base.
     *
     * @param uri    the uri
     * @param method the method
     * @return the http entity enclosing request base
     */
    public static HttpEntityEnclosingRequestBase create(URI uri, HttpSinkRequestMethodType method) {
        switch (method) {
            case POST:
                return new HttpPost(uri);
            case PATCH:
                return new HttpPatch(uri);
            default:
                return new HttpPut(uri);
        }
    }
}
