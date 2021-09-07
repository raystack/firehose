# Stencil Client

Stencil, the Protobuf schema registry used by Firehose need the following variables to be set for the Stencil client.

## `SCHEMA_REGISTRY_STENCIL_ENABLE`

Defines whether to enable Stencil Schema registry

* Example value: `true`
* Type: `optional`
* Default value: `false`

## `SCHEMA_REGISTRY_STENCIL_URLS`

Defines the URL of the Proto Descriptor set file in the Stencil Server

* Example value: `http://localhost:8000/v1/namespaces/quickstart/descriptors/example/versions/latest`
* Type: `optional`

## `SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS`

Defines the timeout in milliseconds to fetch the Proto Descriptor set file from the Stencil Server.

* Example value: `4000`
* Type: `optional`
* Default value: `10000`

## `SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES`

Defines the number of times to retry to fetch the Proto Descriptor set file from the Stencil Server.

* Example value: `4`
* Type: `optional`
* Default value: `3`

## `SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS`

Defines the minimum time in milliseconds after which to back off from fetching the Proto Descriptor set file from the Stencil Server.

* Example value: `70000`
* Type: `optional`
* Default value: `60000`

## `SCHEMA_REGISTRY_STENCIL_FETCH_AUTH_BEARER_TOKEN`

Defines the token for authentication to connect to Stencil Server

* Example value: `tcDpw34J8d1`
* Type: `optional`

## `SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH`

Defines whether to enable auto-refresh of Stencil cache.

* Example value: `true`
* Type: `optional`
* Default value: `false`

## `SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS`

Defines the minimum time in milliseconds after which to refresh the Stencil cache.

* Example value: `900000`
* Type: `optional`
* Default value: `900000`

