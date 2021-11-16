# DLQ

DLQ storage can be configured for certain errors thrown by sink.

## `DLQ_SINK_ENABLE`

* Example value: `true`
* Type: `optional`
* Default value: `false`

## `DLQ_WRITER_TYPE`

DLQ Writer to be configured. The possible values are, `KAFKA,BLOB_STORAGE,LOG`

* Example value: `BLOB_STORAGE`
* Type: `optional`
* Default value: `LOG`

## `DLQ_RETRY_MAX_ATTEMPTS`

Max attempts to retry for dlq.

* Example value: `3`
* Type: `optional`
* Default value: `2147483647`

## `DLQ_RETRY_FAIL_AFTER_MAX_ATTEMPT_ENABLE`

* Example value: `true`
* Type: `optional`
* Default value: `false`

## `DLQ_BLOB_STORAGE_TYPE`

If the writer type is set to BLOB_STORAGE, we can choose any blob storage. Currently, only GCS is supported.

* Example value: `GCS`
* Type: `optional`
* Default value: `GCS`

## `DLQ_GCS_GOOGLE_CLOUD_PROJECT_ID`

* Example value: `my-project-id`
* Type: `Required if BLOB storage type is GCS`

## `DLQ_GCS_BUCKET_NAME`

* Example value: `dlq-bucket`
* Type: `Required if BLOB storage type is GCS`

## `DLQ_GCS_CREDENTIAL_PATH`

* Example value: `/path/for/json/credential`
* Type: `Required if BLOB storage type is GCS`

## `DLQ_GCS_RETRY_MAX_ATTEMPTS`

* Example value: `3`
* Type: `optional`
* Default value: `10`

## `DLQ_GCS_RETRY_TOTAL_TIMEOUT_MS`

* Example value: `120000`
* Type: `optional`
* Default value: `120000`

## `DLQ_GCS_RETRY_INITIAL_DELAY_MS`

* Example value: `1000`
* Type: `optional`
* Default value: `1000`

## `DLQ_GCS_RETRY_MAX_DELAY_MS`

* Example value: `30000`
* Type: `optional`
* Default value: `30000`

## `DLQ_GCS_RETRY_DELAY_MULTIPLIER`

* Example value: `2`
* Type: `optional`
* Default value: `2`

## `DLQ_GCS_RETRY_INITIAL_RPC_TIMEOUT_MS`

* Example value: `5000`
* Type: `optional`
* Default value: `5000`

## `DLQ_GCS_RETRY_RPC_TIMEOUT_MULTIPLIER`

* Example value: `1`
* Type: `optional`
* Default value: `1`

## `DLQ_GCS_RETRY_RPC_MAX_TIMEOUT_MS`

* Example value: `5000`
* Type: `optional`
* Default value: `5000`

## `DLQ_KAFKA_ACKS`

* Example value: `all`
* Type: `optional`
* Default value: `all`

## `DLQ_KAFKA_RETRIES`

* Example value: `3`
* Type: `optional`
* Default value: `2147483647`

## `DLQ_KAFKA_BATCH_SIZE`

* Example value: `100`
* Type: `optional`
* Default value: `16384`

## `DLQ_KAFKA_LINGER_MS`

* Example value: `5`
* Type: `optional`
* Default value: `0`

## `DLQ_KAFKA_BUFFER_MEMORY`

* Example value: `33554432`
* Type: `optional`
* Default value: `33554432`

## `DLQ_KAFKA_KEY_SERIALIZER`

* Example value: `your.own.class`
* Type: `optional`
* Default value: `org.apache.kafka.common.serialization.ByteArraySerializer`

## `DLQ_KAFKA_VALUE_SERIALIZER`

* Example value: `your.own.class`
* Type: `optional`
* Default value: `org.apache.kafka.common.serialization.ByteArraySerializer`

## `DLQ_KAFKA_BROKERS`

* Example value: `127.0.0.1:1234`
* Type: `required if writer type is kafka`

## `DLQ_KAFKA_TOPIC`

* Example value: `your-own-topic`
* Type: `optional`
* Default value: `firehose-retry-topic`
