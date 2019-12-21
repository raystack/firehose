package com.gojek.esb.latestSink.influxdb;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.InfluxSinkConfig;
import com.gojek.esb.latestSink.AbstractSink;
import com.gojek.esb.latestSink.SinkFactory;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import java.util.Map;

public class InfluxSinkFactory implements SinkFactory {
    @Override
    public AbstractSink create(Map<String, String> configProperties, StatsDReporter statsDReporter, StencilClient stencilClient) {
        InfluxSinkConfig config = ConfigFactory.create(InfluxSinkConfig.class, configProperties);
        InfluxDB client = InfluxDBFactory.connect(config.getDbUrl(), config.getUser(), config.getPassword());
        return new InfluxSink(new Instrumentation(statsDReporter, InfluxSink.class), "influx.db", config, new ProtoParser(stencilClient, config.getProtoSchema()), client, stencilClient);
    }
}
