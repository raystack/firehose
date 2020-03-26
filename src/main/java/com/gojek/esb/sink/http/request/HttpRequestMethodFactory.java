package com.gojek.esb.sink.http.request;

import com.gojek.esb.config.enums.HttpRequestMethod;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;

import java.net.URI;

public class HttpRequestMethodFactory {
    public static HttpEntityEnclosingRequestBase create(URI uri, HttpRequestMethod method) {
        if (method.equals(HttpRequestMethod.POST)) {
            return new HttpPost(uri);
        } else {
            return new HttpPut(uri);
        }
    }
}
