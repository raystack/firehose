package org.raystack.firehose.sink.http.request.create;

import org.raystack.firehose.message.Message;
import org.raystack.firehose.sink.http.request.entity.RequestEntityBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

/**
 * Creates http requests.
 */
public interface RequestCreator {

    List<HttpEntityEnclosingRequestBase> create(List<Message> bodyContents, RequestEntityBuilder entity) throws URISyntaxException;
}
