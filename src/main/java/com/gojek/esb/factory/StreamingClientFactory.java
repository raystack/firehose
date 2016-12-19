package com.gojek.esb.factory;

import com.gojek.esb.config.StreamingConfig;
import com.gojek.esb.consumer.StreamingClient;
import com.gojek.esb.sink.HttpSink;
import com.gojek.esb.sink.Sink;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamingClientFactory {

    private static final Logger logger = LoggerFactory.getLogger(StreamingClientFactory.class.getName());

    private static final Sink sink = new HttpSink(FactoryUtils.httpClient);

    private static final StreamingConfig streamingConfig = ConfigFactory.create(StreamingConfig.class, System.getenv());

    public static StreamingClient getStreamingClient() {
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, streamingConfig.getApplicationId());
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, FactoryUtils.appConfig.getKafkaAddress());
        streamsConfig.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, streamingConfig.getZookeeperConnect());
        streamsConfig.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamingConfig.getStreamThreads());
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, streamingConfig.getCommitIntervalInMs());

        StreamingClient streamingClient = new StreamingClient(streamsConfig, sink, streamingConfig.getStreamingTopics().split(","),
                streamingConfig.getBatchSize(), streamingConfig.getFlushIntervalInMs());

        return streamingClient;
    }
}