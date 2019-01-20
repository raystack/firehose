package com.gojek.esb.sink.influxdb;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.config.InfluxSinkConfig;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.parser.ProtoParser;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.SinkFactory;
import org.aeonbits.owner.ConfigFactory;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import java.util.Map;

public class InfluxSinkFactory implements SinkFactory {
    @Override
    public Sink create(Map<String, String> configProperties, StatsDReporter statsDReporter, StencilClient stencilClient) {
        InfluxSinkConfig config = ConfigFactory.create(InfluxSinkConfig.class, configProperties);
        InfluxDB client = InfluxDBFactory.connect(config.getDbUrl(), config.getUser(), config.getPassword());
        return new InfluxSink(client, new ProtoParser(stencilClient, config.getProtoSchema()), config, statsDReporter, stencilClient);
    }
}
