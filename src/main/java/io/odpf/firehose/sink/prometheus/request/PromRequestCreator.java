package io.odpf.firehose.sink.prometheus.request;

import com.gojek.de.stencil.parser.ProtoParser;
import io.odpf.firehose.config.PrometheusSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.prometheus.builder.HeaderBuilder;
import io.odpf.firehose.sink.prometheus.builder.RequestEntityBuilder;
import io.odpf.firehose.sink.prometheus.builder.TimeSeriesBuilder;
import io.odpf.firehose.sink.prometheus.builder.WriteRequestBuilder;

/**
 * Prometheus Request Creator.
 */
public class PromRequestCreator {
    private PrometheusSinkConfig prometheusSinkConfig;
    private StatsDReporter statsDReporter;
    private ProtoParser protoParser;


    /**
     * Instantiates a new Prometheus request creator.
     *
     * @param statsDReporter        the statsd reporter
     * @param prometheusSinkConfig  the configuration for prometheus sink
     * @param protoParser           the proto parser
     */
    public PromRequestCreator(StatsDReporter statsDReporter, PrometheusSinkConfig prometheusSinkConfig, ProtoParser protoParser) {
        this.statsDReporter = statsDReporter;
        this.prometheusSinkConfig = prometheusSinkConfig;
        this.protoParser = protoParser;
    }

    /**
     * Instantiates a new Prometheus request.
     *
     * @return PromRequest
     */
    public PromRequest createRequest() {
        WriteRequestBuilder body = createBody();
        HeaderBuilder headerBuilder = new HeaderBuilder(prometheusSinkConfig.getSinkPromHeaders());
        String baseUrl = prometheusSinkConfig.getSinkPromServiceUrl();
        RequestEntityBuilder requestEntityBuilder = new RequestEntityBuilder();

        return new PromRequest(new Instrumentation(statsDReporter, PromRequest.class),
                headerBuilder, baseUrl, requestEntityBuilder, body);
    }

    /**
     * create prometheus writeRequest builder.
     *
     * @return WriteRequestBuilder
     */
    private WriteRequestBuilder createBody() {
        TimeSeriesBuilder timeSeriesBuilder = new TimeSeriesBuilder(prometheusSinkConfig);
        return new WriteRequestBuilder(timeSeriesBuilder, protoParser);
    }
}
