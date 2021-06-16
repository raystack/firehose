package io.odpf.firehose.sink.bq;

import com.gojek.de.stencil.client.StencilClient;
import io.odpf.firehose.config.BQSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

public class BQSinkFactory {
    private ProtoUpdateListener protoUpdateListener;

    public Sink create(Map<String, String> config, StatsDReporter statsDReporter, StencilClient stencilClient) {
        BQSinkConfig bqSinkConfig = ConfigFactory.create(BQSinkConfig.class, config);
        Instrumentation instrumentation = new Instrumentation(statsDReporter, BQSinkFactory.class);

        this.protoUpdateListener = new ProtoUpdateListener();
        return new BQSink();
    }
}
