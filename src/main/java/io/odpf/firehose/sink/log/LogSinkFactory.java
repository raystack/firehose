package io.odpf.firehose.sink.log;


import io.odpf.firehose.config.AppConfig;
import io.odpf.firehose.config.enums.InputSchemaDataType;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.parser.json.JsonMessageParser;
import io.odpf.firehose.sink.Sink;
import io.odpf.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the LogSink.
 * <p>
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient client)}
 * to obtain the LogSink sink implementation.
 */
public class LogSinkFactory {

    /**
     * Creates the LogSink.
     *
     * @param configuration  the configuration
     * @param statsDReporter the stats d reporter
     * @param stencilClient  the stencil client
     * @return the sink
     */
    public static Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, configuration);
        InputSchemaDataType inputSchemaDataTye = appConfig.getInputSchemaDataTye();
        switch (inputSchemaDataTye) {
            case JSON:
                return new LogSinkforJson(new Instrumentation(statsDReporter, LogSinkforJson.class), new JsonMessageParser(appConfig));
            case PROTOBUF:
            default:
                KeyOrMessageParser parser = new KeyOrMessageParser(stencilClient.getParser(appConfig.getInputSchemaProtoClass()), appConfig);
                return new LogSink(parser, new Instrumentation(statsDReporter, LogSink.class));
        }
    }
}
