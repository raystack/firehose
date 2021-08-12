# Kafka Consumer

## `SOURCE_KAFKA_BROKERS`

Defines the bootstrap server of Kafka brokers to consume from.

* Example value: `localhost:9092`
* Type: `required`

## `SOURCE_KAFKA_TOPIC`

Defines the list of Kafka topics to consume from.

* Example value: `test-topic`
* Type: `required`

## `SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS`

Defines the batch size of Kafka messages

* Example value: `705`
* Type: `optional`
* Default value: `500`

## `SOURCE_KAFKA_ASYNC_COMMIT_ENABLE`

Defines whether to enable async commit for Kafka consumer

* Example value: `false`
* Type: `optional`
* Default value: `true`

## `SOURCE_KAFKA_CONSUMER_CONFIG_SESSION_TIMEOUT_MS`

Defines the duration of session timeout in milliseconds

* Example value: `700`
* Type: `optional`
* Default value: `10000`

## `SOURCE_KAFKA_COMMIT_ONLY_CURRENT_PARTITIONS_ENABLE`

Defines whether to commit only current partitions

* Example value: `false`
* Type: `optional`
* Default value: `true`

## `SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE`

Defines whether to enable auto commit for Kafka consumer

* Example value: `705`
* Type: `optional`
* Default value: `500`

## `SOURCE_KAFKA_CONSUMER_GROUP_ID`

Defines the Kafka consumer group ID for your Firehose deployment.

* Example value: `sample-group-id`
* Type: `required`

## `SOURCE_KAFKA_POLL_TIMEOUT_MS`

Defines the duration of poll timeout for Kafka messages in milliseconds

* Example value: `80000`
* Type: `required`
* Default: `9223372036854775807`

## `SOURCE_KAFKA_CONSUMER_CONFIG_METADATA_MAX_AGE_MS`

Defines the maximum age of config metadata in milliseconds

* Example value: `700`
* Type: `optional`
* Default value: `500`

