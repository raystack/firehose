package com.gojek.esb.config;

public interface ExponentialBackOffProviderConfig extends AppConfig {

    @Key("retry.exponential.backoff.initial.ms")
    @DefaultValue("10")
    Integer getRetryExponentialBackoffInitialMs();

    @Key("retry.exponential.backoff.rate")
    @DefaultValue("2")
    Integer getRetryExponentialBackoffRate();

    @Key("retry.exponential.backoff.max.ms")
    @DefaultValue("60000")
    Integer getRetryExponentialBackoffMaxMs();
}
