## Concepts
#### Architecture
Firehose has the capability to run parallelly on threads. Each thread does the following:
* Get messages from Kafka
* Filter the messages (optional)
* Push these messages to sink
* In case push fails and retry queue is:
    * enabled: Firehose keeps on retrying for configured number of attempts before the messages got pushed to retry queue kafka topic
    * disabled: Firehose keeps on retrying until it receives a success code
* Captures telemetry and success/failure events and send them to Telegraf
* Repeat the process

![Firehose Architecture](https://github.com/odpf/firehose/tree/main/docs/images/architecture.png)

#### System Design
**Components**

***Consumer***
* Firehose Consumer consumes messages from the configured Kafka in batches, `source.kafka.consumer.config.max.poll.records` can be configured which decides this batch size.
* The consumer then processes each message and sends the messages’ list to Filter.

***Filter***
* Here it looks for any filters that are configured while creating firehose.
* There can be a filter on either Key or on Message depending on which fields you want to apply filters on.
* One can choose not to apply any filters and send all the records to the sink.
* It will apply the provided filter condition on the incoming batch of messages and pass the list of filtered messages to the Sink class for the configured sink type.

***Sink***
* Firehose has an exclusive Sink class for each of the sink types, this Sink receives a list of filtered messages that will go through a well-defined lifecycle.
* All the existing sink types follow the same contract/lifecycle defined in `AbstractSink.java`. It consists of two stages:
    * Prepare: Transformation over filtered messages’ list to prepare the sink specific insert/update client requests.
    * Execute: Requests created in the Prepare stage are executed at this step and a list of failed messages is returned (if any) for retry.
* If the batch has any failures, Firehose will retry to push the failed messages to the sink

***Instrumentation***
* Instrumentation is a wrapper around statsDclient and logging. Its job is to capture Important metrics such as Latencies, Successful/Failed Messages Count, Sink Response Time, etc. for each record that goes through the firehose ecosystem.

***Commit***
* After the messages are sent successfully, Firehose commits the offset, and the consumer polls another batch from Kafka.

**Schema Handling**
* Protocol buffers are Google's language-neutral, platform-neutral, extensible mechanism for serializing structured data. Data streams on Kafka topics are bound to a protobuf schema.
* Firehose deserializes the data consumed from the topics using the Protobuf descriptors generated out of the artifacts. The schema handling ie., find the mapped schema for the topic, downloading the descriptors, and dynamically being notified of/updating with the latest schema is abstracted through the Stencil library.
The stencil is a proprietary library that provides an abstraction layer, for schema handling.
Schema Caching, dynamic schema updates are features of the stencil client library.

#### Firehose Integration
The section details all integrating systems for Firehose deployment. These are external systems that Firehose connects to.

![Firehose Integration](https://github.com/odpf/firehose/tree/main/docs/images/integration.png)

**Kafka**
* The Kafka topic(s) where Firehose reads from. The `source.kafka.topic` config can be set in Firehose

**ProtoDescriptors**
* Generated protobuf descriptors which are hosted behind an artifactory/HTTP endpoint. This endpoint URL and the proto that the firehose deployment should use to deserialize data with is configured in firehose.

**Telegraf**
* Telegraf is run as a process beside Firehose to export metrics to InfluxDB. Firehose internally uses statsd, a java library to export metrics to telegraf. Configured with statsd host parameter that Firehose points. 

**Sink**
* The storage where Firehose eventually pushes the data. Can be an HTTP/GRPC Endpoint or one of the Databases mentioned in the Architecture section. Sink Type and each sink-specific configuration are relevant to this integration point.

**InfluxDB**
* InfluxDB - time-series database where all firehose metrics are stored. Integration through the Telegraf component.

For a complete set of configuration please refer to the sink-specific configuration in How-to guides.
