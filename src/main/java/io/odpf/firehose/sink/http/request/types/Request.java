package io.odpf.firehose.sink.http.request.types;

import io.odpf.firehose.config.HttpSinkConfig;
import io.odpf.firehose.config.enums.HttpSinkDataFormatType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.sink.http.request.entity.RequestEntityBuilder;
import io.odpf.firehose.sink.http.request.header.HeaderBuilder;
import io.odpf.firehose.sink.http.request.uri.UriBuilder;
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
