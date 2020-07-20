package com.gojek.esb.sink.http.request.create;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.sink.http.request.entity.EntityBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

public interface RequestCreator {

    List<HttpEntityEnclosingRequestBase> create(List<EsbMessage> bodyContents, EntityBuilder entity) throws URISyntaxException;
}
