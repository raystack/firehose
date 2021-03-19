package io.odpf.firehose.sink.influxdb;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import io.odpf.firehose.config.InfluxSinkConfig;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.SinkFactory;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import java.util.Map;

public class InfluxSinkFactory implements SinkFactory {
    @Override
    public AbstractSink create(Map<String, String> configProperties, StatsDReporter statsDReporter, StencilClient stencilClient) {
        InfluxSinkConfig config = ConfigFactory.create(InfluxSinkConfig.class, configProperties);

        Instrumentation instrumentation = new Instrumentation(statsDReporter, InfluxSinkFactory.class);
        instrumentation.logDebug("\nInflux Url: {}\nInflux Username: {}", config.getSinkInfluxUrl(), config.getSinkInfluxUsername());

        InfluxDB client = InfluxDBFactory.connect(config.getSinkInfluxUrl(), config.getSinkInfluxUsername(), config.getSinkInfluxPassword());
        instrumentation.logInfo("InfluxDB connection established");

        return new InfluxSink(new Instrumentation(statsDReporter, InfluxSink.class), "influx.db", config, new ProtoParser(stencilClient, config.getInputSchemaProtoClass()), client, stencilClient);
    }
}
