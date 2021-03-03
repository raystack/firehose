# Introduction
Firehose is a cloud native service for delivering real-time streaming data to destinations such as service endpoints (HTTP or GRPC) & managed databases (Postgres, InfluxDB,  Redis, & ElasticSearch). With Firehose, you don't need to write applications or manage resources. It automatically scales to match the throughput of your data and requires no ongoing administration. If your data is present in Kafka, Firehose delivers it to the destination(SINK) that you specified.

<p align="center"><img src="./assets/overview.svg" /></p>

## Key Features
Discover why users choose Firehose as their main Kafka Consumer

**Support for Multiple Sinks**  Firehose supports sinking stream data to log console, HTTP, GRPC, PostgresDB, InfluxDB, ElasticSearch, Redis & Clevertap
**Self-serve** Configure, deploy, validate, scale, monitor, alert, debug, audit and so on. Every action is DIY
**Elastic Scaling** Cloud-native & leverages horizontal scaling and high-performance streaming to sink data in near real-time & zero data drops
**Metrics for Monitoring Performance** Exposes critical metrics through the consoles to monitor the health of your delivery streams, take any necessary actions

## Supported Sinks:
Following sinks are supported in firehose
* [Log](https://en.wikipedia.org/wiki/Log_file)
* [HTTP](https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol)
* [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity)
* [InfluxDB](https://en.wikipedia.org/wiki/InfluxDB)
* [Redis](https://en.wikipedia.org/wiki/Redis)
* [ElasticSearch](https://en.wikipedia.org/wiki/Elasticsearch)
* [GRPC](https://en.wikipedia.org/wiki/GRPC)
* [Prometheus](https://en.wikipedia.org/wiki/Prometheus_(software)) - `Coming soon`

## How is Firhose different from Kafka-connect?
#### Kafka-Connect
PROS
* Can consume from Kafka as well as many other sources
* Easier to integrate if working with confluent-platform
* Provides a bunch of transformation options as part of Single Message Transformations(SMTs)

CONS
* When using in distributed mode across multiple nodes, need to install all the connectors across all the workers within your kafka connect cluster
* Available connectors may have some limitations. Its usually rare to find all the required features in a single connector and so is to find a documentation for the same
* Separation of commercial and open-source features is very poor
* For monitoring and many other features confluent control center asks for an enterprise subscription
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#### Firehose
PROS
* Easier to install and use different sinks by tweaking only a couple of configurations
* Comes with tons of exclusive features for each sink. e.g. JSON body template, Parameterised header/request, OAuth2 for HTTP sink
* Value based filtering is much easier to implement as compared to Kafka-Connect. Requires no additional plugins/schema-registry to be installed
* Provides a comprehensible Abstract sink contract which makes it Easier to add a new sink in Firehose
* Don't need to think about converters and serializers, Firehose comes with an inbuilt serialization/deserialization
* Comes with Firehose health dashboard (Grafana) for effortless monitoring free of cost

CONS
* Can consume from Kafka but not from any other data source
* Supports only protobuf format as of now
* Doesn't support Kafka Sink yet

## How can I get started?

Explore the following resources to get started with Firehose:

* [Guides](guides/overview.md) provides guidance on creating Firehose with different sinks.
* [Concepts](concepts/overview.md) describes all important Firehose concepts.
* [Reference](reference/) contains details about configurations, metrics and other aspects of Firehose.
* [Contributing](contribute/contributing.md) contains resources for anyone who wants to contribute to Firehose.
