package com.gojek.esb.sink.http.request;

import com.gojek.esb.config.enums.HttpSinkRequestMethodType;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;

import java.net.URI;

public class HttpRequestMethodFactory {
    public static HttpEntityEnclosingRequestBase create(URI uri, HttpSinkRequestMethodType method) {
        if (method.equals(HttpSinkRequestMethodType.POST)) {
            return new HttpPost(uri);
        } else {
            return new HttpPut(uri);
        }
    }
}
