package com.gojek.esb.launch;

import com.gojek.esb.config.DBConfig;
import com.gojek.esb.config.SinkType;
import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.consumer.StreamingClient;
import com.gojek.esb.factory.FactoryUtils;
import com.gojek.esb.factory.LogConsumerFactory;
import com.gojek.esb.factory.StreamingClientFactory;
import com.gojek.esb.sink.DBSink;
import com.gojek.esb.sink.GenericMessageFieldAccessor;
import com.gojek.esb.sink.HttpSink;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.util.DriverManagerUtil;
import org.aeonbits.owner.ConfigFactory;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {

        Sink sink = null;

        if (FactoryUtils.appConfig.getSinkType() == SinkType.DB) {
            DBConfig dbConfig = ConfigFactory.create(DBConfig.class, System.getenv());
            DriverManagerUtil driverManagerUtil = new DriverManagerUtil();
            GenericMessageFieldAccessor genericMessageFieldAccessor = new GenericMessageFieldAccessor(dbConfig.getProtoSchema());
            sink = new DBSink(dbConfig, driverManagerUtil, genericMessageFieldAccessor);
        } else if (FactoryUtils.appConfig.getSinkType() == SinkType.HTTP) {
            sink = new HttpSink(FactoryUtils.httpClient);
        }
        sink.initialize();

        if (!FactoryUtils.appConfig.isStreaming()) {
            LogConsumer logConsumer = LogConsumerFactory.getLogConsumer(sink);

            while (true) {
                logConsumer.processPartitions();
            }
        } else {
            StreamingClient streamingClient = StreamingClientFactory.getStreamingClient();
            streamingClient.start();
        }
    }
}
