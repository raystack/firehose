package io.odpf.firehose.sink.http.request.types;

import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.firehose.config.HttpSinkConfig;
import io.odpf.firehose.config.enums.HttpSinkRequestMethodType;
import io.odpf.firehose.config.enums.HttpSinkParameterSourceType;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.sink.http.request.body.JsonBody;
import io.odpf.firehose.sink.http.request.create.IndividualRequestCreator;
import io.odpf.firehose.sink.http.request.create.RequestCreator;
import io.odpf.firehose.sink.http.request.entity.RequestEntityBuilder;
import io.odpf.firehose.sink.http.request.header.HeaderBuilder;
import io.odpf.firehose.sink.http.request.uri.UriBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

/**
 * Dynamic url request.
 */
public class DynamicUrlRequest implements Request {

    private StatsDReporter statsDReporter;
    private HttpSinkConfig httpSinkConfig;
    private JsonBody body;
    private HttpSinkRequestMethodType method;
    private RequestEntityBuilder requestEntityBuilder;
    private RequestCreator requestCreator;

    /**
     * Instantiates a new Dynamic url request.
     *
     * @param statsDReporter the stats d reporter
     * @param httpSinkConfig the http sink config
     * @param body           the body
     * @param method         the method
     */
    public DynamicUrlRequest(StatsDReporter statsDReporter, HttpSinkConfig httpSinkConfig, JsonBody body, HttpSinkRequestMethodType method) {
        this.statsDReporter = statsDReporter;
        this.httpSinkConfig = httpSinkConfig;
        this.body = body;
        this.method = method;
    }

    public List<HttpEntityEnclosingRequestBase> build(List<Message> messages) throws DeserializerException, URISyntaxException {
        return requestCreator.create(messages, requestEntityBuilder.setWrapping(!isTemplateBody(httpSinkConfig)));
    }

    /**
     * Sets request strategy.
     *
     * @param headerBuilder        the header builder
     * @param uriBuilder           the uri builder
     * @param requestEntitybuilder the request entitybuilder
     * @return the request strategy
     */
    @Override
    public Request setRequestStrategy(HeaderBuilder headerBuilder, UriBuilder uriBuilder, RequestEntityBuilder requestEntitybuilder) {
        this.requestCreator = new IndividualRequestCreator(
                new FirehoseInstrumentation(statsDReporter, IndividualRequestCreator.class), uriBuilder, headerBuilder, method, body, httpSinkConfig);
        this.requestEntityBuilder = requestEntitybuilder;
        return this;
    }

    @Override
    public boolean canProcess() {
        boolean isDynamicUrl = httpSinkConfig.getSinkHttpServiceUrl().contains(",");
        return httpSinkConfig.getSinkHttpParameterSource() == HttpSinkParameterSourceType.DISABLED && isDynamicUrl;
    }
}
