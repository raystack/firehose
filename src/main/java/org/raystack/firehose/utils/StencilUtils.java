package org.raystack.firehose.utils;

import org.raystack.firehose.config.AppConfig;
import com.timgroup.statsd.StatsDClient;
import org.raystack.stencil.SchemaUpdateListener;
import org.raystack.stencil.config.StencilConfig;

public class StencilUtils {
    public static StencilConfig getStencilConfig(
            AppConfig appconfig,
            StatsDClient statsDClient,
            SchemaUpdateListener schemaUpdateListener) {
        return StencilConfig.builder()
                .cacheAutoRefresh(appconfig.getSchemaRegistryStencilCacheAutoRefresh())
                .cacheTtlMs(appconfig.getSchemaRegistryStencilCacheTtlMs())
                .statsDClient(statsDClient)
                .fetchHeaders(appconfig.getSchemaRegistryFetchHeaders())
                .fetchBackoffMinMs(appconfig.getSchemaRegistryStencilFetchBackoffMinMs())
                .fetchRetries(appconfig.getSchemaRegistryStencilFetchRetries())
                .fetchTimeoutMs(appconfig.getSchemaRegistryStencilFetchTimeoutMs())
                .refreshStrategy(appconfig.getSchemaRegistryStencilRefreshStrategy())
                .updateListener(schemaUpdateListener)
                .build();
    }

    public static StencilConfig getStencilConfig(AppConfig appconfig, StatsDClient statsDClient) {
        return getStencilConfig(appconfig, statsDClient, null);
    }
}
