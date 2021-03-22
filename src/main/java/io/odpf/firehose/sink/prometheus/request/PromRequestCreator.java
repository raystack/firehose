package com.gojek.esb.sink.prometheus.request;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.PrometheusSinkConfig;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.http.request.header.HeaderBuilder;
import com.gojek.esb.sink.http.request.uri.UriBuilder;
import com.gojek.esb.sink.http.request.uri.UriParser;
import com.gojek.esb.sink.prometheus.builder.RequestEntityBuilder;
import com.gojek.esb.sink.prometheus.builder.TimeSeriesBuilder;
import com.gojek.esb.sink.prometheus.builder.WriteRequestBuilder;


public class PromRequestCreator {
    private PrometheusSinkConfig prometheusSinkConfig;
    private UriParser uriParser;
    private StatsDReporter statsDReporter;
    private ProtoParser protoParser;


    public PromRequestCreator(StatsDReporter statsDReporter, PrometheusSinkConfig prometheusSinkConfig, ProtoParser protoParser, UriParser uriParser) {
        this.statsDReporter = statsDReporter;
        this.prometheusSinkConfig = prometheusSinkConfig;
        this.protoParser = protoParser;
        this.uriParser = uriParser;
    }

    public PromRequest createRequest() {
        WriteRequestBuilder body = createBody();
        HeaderBuilder headerBuilder = new HeaderBuilder(prometheusSinkConfig.getSinkPromHeaders());
        UriBuilder uriBuilder = new UriBuilder(prometheusSinkConfig.getSinkPromServiceUrl(), uriParser);
        RequestEntityBuilder requestEntityBuilder = new RequestEntityBuilder();

        return new PromRequest(new Instrumentation(statsDReporter, PromRequest.class),
                headerBuilder, uriBuilder, requestEntityBuilder, body);
    }

    private WriteRequestBuilder createBody() {
        TimeSeriesBuilder timeSeriesBuilder = new TimeSeriesBuilder(prometheusSinkConfig);
        return new WriteRequestBuilder(timeSeriesBuilder, protoParser);
    }
}
