package com.gojek.esb.sink.http.request.types;

import com.gojek.esb.config.HttpSinkConfig;
import com.gojek.esb.config.enums.HttpSinkDataFormatType;
import com.gojek.esb.consumer.Message;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.http.request.entity.RequestEntityBuilder;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.UriBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

public interface Request {
    List<HttpEntityEnclosingRequestBase> build(List<Message> messages) throws URISyntaxException, DeserializerException;

    Request setRequestStrategy(HeaderBuilder headerBuilder, UriBuilder uriBuilder, RequestEntityBuilder requestEntityBuilder);

    boolean canProcess();

    default boolean isTemplateBody(HttpSinkConfig httpSinkConfig) {
        return httpSinkConfig.getSinkHttpDataFormat() == HttpSinkDataFormatType.JSON
                && !httpSinkConfig.getSinkHttpJsonBodyTemplate().isEmpty();
    }
}
