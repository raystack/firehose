# Architecture

![Firehose Architecture](../.gitbook/assets/architecture.png)

Firehose has the capability to run parallelly on threads. Each thread does the following:

* Get messages from Kafka
* Filter the messages \(optional\)
* Push these messages to sink
* In case push fails and DLQ is:
  * enabled: Firehose keeps on retrying for the configured number of attempts before the messages got pushed to DLQ Kafka topic
  * disabled: Firehose keeps on retrying until it receives a success code
* Captures telemetry and success/failure events and send them to Telegraf
* Repeat the process

## System Design

### Components

_**Consumer**_

* Firehose Consumer consumes messages from the configured Kafka in batches, [`SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS`](../reference/configuration.md#source_kafka_consumer_config_max_poll_records) can be configured which decides this batch size.
* The consumer then processes each message and sends the messages’ list to Filter.

_**Filter**_

* Here it looks for any filters that are configured while creating Firehose.
* There can be a filter on either Key or on Message depending on which fields you want to apply filters on.
* One can choose not to apply any filters and send all the records to the sink.
* It will apply the provided filter condition on the incoming batch of messages and pass the list of filtered messages to the Sink class for the configured sink type.

_**Sink**_

* Firehose has an exclusive Sink class for each of the sink types, this Sink receives a list of filtered messages that will go through a well-defined lifecycle.
* All the existing sink types follow the same contract/lifecycle defined in `AbstractSink.java`. It consists of two stages:
  * Prepare: Transformation over-filtered messages’ list to prepare the sink-specific insert/update client requests.
  * Execute: Requests created in the Prepare stage are executed at this step and a list of failed messages is returned \(if any\) for retry.
* If the batch has any failures, Firehose will retry to push the failed messages to the sink

_**Instrumentation**_

* Instrumentation is a wrapper around statsD client and logging. Its job is to capture Important metrics such as Latencies, Successful/Failed Messages Count, Sink Response Time, etc. for each record that goes through the Firehose ecosystem.

_**Commit**_

* After the messages are sent successfully, Firehose commits the offset, and the consumer polls another batch from Kafka.

### Schema Handling

* Protocol buffers are Google's language-neutral, platform-neutral, extensible mechanism for serializing structured data. Data streams on Kafka topics are bound to a protobuf schema.
* Firehose deserializes the data consumed from the topics using the Protobuf descriptors generated out of the artifacts. The artifactory is an HTTP interface that Firehose uses to deserialize. 
* The schema handling ie., find the mapped schema for the topic, downloading the descriptors, and dynamically being notified of/updating with the latest schema is abstracted through the Stencil library.

  The Stencil is a proprietary library that provides an abstraction layer, for schema handling.

  Schema Caching, dynamic schema updates, etc. are features of the stencil client library.

## Firehose Integration

The section details all integrating systems for Firehose deployment. These are external systems that Firehose connects to.

![Firehose Integration](../.gitbook/assets/integration.png)

### Kafka

* The Kafka topic\(s\) where Firehose reads from. The [`SOURCE_KAFKA_TOPIC`](../reference/configuration.md#source_kafka_topic) config can be set in Firehose.

### ProtoDescriptors

* Generated protobuf descriptors which are hosted behind an artifactory/HTTP endpoint. This endpoint URL and the proto that the Firehose deployment should use to deserialize data with is configured in Firehose.

### Telegraf

* Telegraf is run as a process beside Firehose to export metrics to InfluxDB. Firehose internally uses statsd, a java library to export metrics to Telegraf. Configured with statsd host parameter that Firehose points. 

### Sink

* The storage where Firehose eventually pushes the data. Can be an HTTP/GRPC Endpoint or one of the Databases mentioned in the Architecture section. Sink Type and each sink-specific configuration are relevant to this integration point.

### InfluxDB

* InfluxDB - time-series database where all Firehose metrics are stored. Integration through the Telegraf component.

For a complete set of configurations please refer to the sink-specific [configuration](../reference/configuration.md).

