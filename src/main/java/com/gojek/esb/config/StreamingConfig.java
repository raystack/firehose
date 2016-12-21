package com.gojek.esb.config;

import org.aeonbits.owner.Config;

public interface StreamingConfig extends Config {
    @Key("APPLICATION_ID")
    String getApplicationId();

    @Key("ZOOKEEPER_ADDRESS")
    String getZookeeperAddress();

    @Key("STREAM_THREADS")
    @DefaultValue("1")
    Integer getStreamThreads();

    @Key("COMMIT_INTERVAL_IN_MS")
    @DefaultValue("30000")
    Integer getCommitIntervalInMs();

    @Key("BATCH_SIZE")
    @DefaultValue("10")
    Integer getBatchSize();

    @Key("FLUSH_INTERVAL_IN_MS")
    @DefaultValue("5000")
    Integer getFlushIntervalInMs();

    @Key("STREAMING_TOPICS")
    String getStreamingTopics();
}
