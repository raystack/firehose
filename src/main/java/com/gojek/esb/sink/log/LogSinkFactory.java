package com.gojek.esb.sink.log;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.AppConfig;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.SinkFactory;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the LogSink.
 * <p>
 * The esb-log-consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map<String, String> configuration, StatsDClient client)}
 * to obtain the LogSink sink implementation.
 */
public class LogSinkFactory implements SinkFactory {

    public Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, configuration);
        KeyOrMessageParser parser = new KeyOrMessageParser(new ProtoParser(stencilClient, appConfig.getProtoSchema()), appConfig);
        return new LogSink(parser, new ConsoleLogger());
    }
}
