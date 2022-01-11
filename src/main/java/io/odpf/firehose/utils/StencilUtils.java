package io.odpf.firehose.utils;

import com.timgroup.statsd.StatsDClient;
import io.odpf.firehose.config.AppConfig;
import io.odpf.stencil.config.StencilConfig;

public class StencilUtils {
    public static StencilConfig getStencilConfig(AppConfig appconfig, StatsDClient statsDClient) {
        return StencilConfig.builder()
                .cacheAutoRefresh(appconfig.getSchemaRegistryStencilCacheAutoRefresh())
                .cacheTtlMs(appconfig.getSchemaRegistryStencilCacheTtlMs())
                .statsDClient(statsDClient)
                .fetchHeaders(appconfig.getSchemaRegistryFetchHeaders())
                .fetchBackoffMinMs(appconfig.getSchemaRegistryStencilFetchBackoffMinMs())
                .fetchRetries(appconfig.getSchemaRegistryStencilFetchRetries())
                .fetchTimeoutMs(appconfig.getSchemaRegistryStencilFetchTimeoutMs())
                .build();
    }
}
