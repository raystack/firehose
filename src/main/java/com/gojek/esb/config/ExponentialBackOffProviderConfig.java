package com.gojek.esb.config;

public interface ExponentialBackOffProviderConfig extends AppConfig {

    @Key("EXPONENTIAL_BACKOFF_INITIAL_BACKOFF_IN_MS")
    @DefaultValue("10")
    Integer exponentialBackoffInitialTimeInMs();

    @Key("EXPONENTIAL_BACKOFF_RATE")
    @DefaultValue("2")
    Integer exponentialBackoffRate();

    @Key("EXPONENTIAL_BACKOFF_MAXIMUM_BACKOFF_IN_MS")
    @DefaultValue("60000")
    Integer exponentialBackoffMaximumBackoffInMs();
}
