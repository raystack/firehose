# Introduction

Firehose is a cloud-native service for delivering real-time streaming data to destinations such as service endpoints \(HTTP or GRPC\) & managed databases \(Postgres, InfluxDB, Redis, & ElasticSearch\). With Firehose, you don't need to write applications or manage resources. It automatically scales to match the throughput of your data and requires no ongoing administration. If your data is present in Kafka, Firehose delivers it to the destination\(SINK\) that you specified.

![](.gitbook/assets/overview%20%283%29.svg)

## Key Features

Discover why users choose Firehose as their main Kafka Consumer

* **Sinks** Firehose supports sinking stream data to log console, HTTP, GRPC, PostgresDB\(JDBC\), InfluxDB, Elastic Search & Redis.
* **Scale** Firehose scales in an instant, both vertically and horizontally, for high-performance streaming sink and zero data drops.
* **Extensibility** Add your own sink to Firehose with a clearly defined interface or choose from already provided ones.
* **Runtime** Firehose can run inside containers or VMs in a fully managed runtime environment like Kubernetes.
* **Metrics** Always know what’s going on with your deployment with built-in monitoring of throughput, response times, errors, and more.

## Supported Sinks:

Following sinks are supported in the Firehose

* [Log](https://en.wikipedia.org/wiki/Log_file) - Standard Output
* [HTTP](https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol) - HTTP services
* [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity) - Postgres DB
* [InfluxDB](https://en.wikipedia.org/wiki/InfluxDB) - A time-series database 
* [Redis](https://en.wikipedia.org/wiki/Redis) - An in-memory Key value store
* [ElasticSearch](https://en.wikipedia.org/wiki/Elasticsearch) - A search database
* [GRPC](https://en.wikipedia.org/wiki/GRPC) - GRPC based services
* [Prometheus](https://en.wikipedia.org/wiki/Prometheus_%28software) - A time-series database

## How is Firehose different from Kafka-Connect?

* **Ease of use:** Firehose is easier to install, and using different sinks only requires changing a few configurations. When used in distributed mode across multiple nodes, it requires connectors to be installed across all the workers within your Kafka-Connect cluster.
* **Filtering:** Value-based filtering is much easier to implement as compared to Kafka-Connect. Requires no additional plugins/schema-registry to be installed.
* **Extensible:** Provides a comprehensible abstract sink contract making it easier to add a new sink in Firehose. Firehose also comes with an inbuilt serialization/deserialization and doesn't require any converters and serializers when implementing a new sink. 
* **Easy monitoring:** Firehose provides a detailed health dashboard \(Grafana\) for effortless monitoring.
* **Connectors:** Some of the Kafka connect available connectors usually have limitations. Its usually rare to find all the required features in a single connector and so is to find documentation for the same
* **Fully open-source:** Firehose is completely open-source while separation of commercial and open-source features is not very structured in Kafka Connect and for monitoring and advanced features, confluent control center requires an enterprise subscription

## How can I get started?

Explore the following resources to get started with Firehose:

* [Guides](guides/overview.md) provide guidance on creating Firehose with different sinks.
* [Concepts](concepts/overview.md) describe all important Firehose concepts.
* [Reference](reference/configuration.md) contains details about configurations, metrics, FAQs, and other aspects of Firehose.
* [Contributing](contribute/contribution.md) contains resources for anyone who wants to contribute to Firehose.

