package io.odpf.firehose.utils;

import io.odpf.firehose.config.AppConfig;
import io.odpf.stencil.config.StencilConfig;

public class StencilUtils {
    public static StencilConfig getStencilConfig(AppConfig appconfig) {
        return StencilConfig.builder()
                .cacheAutoRefresh(appconfig.getSchemaRegistryStencilCacheAutoRefresh())
                .cacheTtlMs(appconfig.getSchemaRegistryStencilCacheTtlMs())
                .fetchAuthBearerToken(appconfig.getSchemaRegistryStencilFetchAuthBearerToken())
                .fetchBackoffMinMs(appconfig.getSchemaRegistryStencilFetchBackoffMinMs())
                .fetchRetries(appconfig.getSchemaRegistryStencilFetchRetries())
                .fetchTimeoutMs(appconfig.getSchemaRegistryStencilFetchTimeoutMs())
                .build();
    }
}
