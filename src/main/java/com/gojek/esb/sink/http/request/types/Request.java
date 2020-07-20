package com.gojek.esb.sink.http.request.types;

import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.config.enums.HttpSinkDataFormat;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.http.request.entity.EntityBuilder;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.URIBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

public interface Request {
    List<HttpEntityEnclosingRequestBase> build(List<EsbMessage> esbMessages) throws URISyntaxException, DeserializerException;

    Request setRequestStrategy(HeaderBuilder headerBuilder, URIBuilder uriBuilder, EntityBuilder entityBuilder);

    boolean canProcess();

    default boolean isTemplateBody(HTTPSinkConfig httpSinkConfig) {
        return httpSinkConfig.getHttpSinkDataFormat() == HttpSinkDataFormat.JSON
                && !httpSinkConfig.getHttpSinkJsonBodyTemplate().isEmpty();
    }
}
