# Stencil Client

## Stencil

### `SCHEMA_REGISTRY_STENCIL_ENABLE`

Defines whether to enable Stencil Schema registry

* Example value: `true`
* Type: `optional`
* Default value: `false`

### `SCHEMA_REGISTRY_STENCIL_URLS`

Defines the URL of the Proto Descriptor set file in the Stencil Server

* Example value: `http://localhost:8000/v1/namespaces/quickstart/descriptors/example/versions/latest`
* Type: `optional`

@Key\("SCHEMA\_REGISTRY\_STENCIL\_FETCH\_TIMEOUT\_MS"\) @DefaultValue\("10000"\) Integer getSchemaRegistryStencilFetchTimeoutMs\(\); @Key\("SCHEMA\_REGISTRY\_STENCIL\_FETCH\_RETRIES"\) @DefaultValue\("4"\) Integer getSchemaRegistryStencilFetchRetries\(\); @Key\("SCHEMA\_REGISTRY\_STENCIL\_FETCH\_BACKOFF\_MIN\_MS"\) @DefaultValue\("60000"\) Long getSchemaRegistryStencilFetchBackoffMinMs\(\); @Key\("SCHEMA\_REGISTRY\_STENCIL\_FETCH\_AUTH\_BEARER\_TOKEN"\) String getSchemaRegistryStencilFetchAuthBearerToken\(\); @Key\("SCHEMA\_REGISTRY\_STENCIL\_CACHE\_AUTO\_REFRESH"\) @DefaultValue\("false"\) Boolean getSchemaRegistryStencilCacheAutoRefresh\(\); @Key\("SCHEMA\_REGISTRY\_STENCIL\_CACHE\_TTL\_MS"\) @DefaultValue\("900000"\) Long getSchemaRegistryStencilCacheTtlMs\(\);

