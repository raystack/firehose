# Introduction

Firehose is a cloud native service for delivering real-time streaming data to destinations such as service endpoints \(HTTP or GRPC\) & managed databases \(Postgres, InfluxDB, Redis, & ElasticSearch\). With Firehose, you don't need to write applications or manage resources. It automatically scales to match the throughput of your data and requires no ongoing administration. If your data is present in Kafka, Firehose delivers it to the destination\(SINK\) that you specified.

## Key Features

Discover why users choose Firehose as their main Kafka Consumer

* **Sinks** Firehose supports sinking stream data to log console, HTTP, GRPC, PostgresDB\(JDBC\), InfluxDB, Elasticsearch & Redis.
* **Scale** Firehose scales in an instant, both vertically and horizontally  for high performance streaming sink and zero data drops.
* **Extensibility** Add your own sink to firehose with a clearly defined interface or choose from already provided ones.
* **Runtime** Firehose can run inside containers or VMs in a fully managed runtime environment like kubernetes.
* **Metrics** Always know what’s going on with your deployment with built-in monitoring of throughput, response times, errors and more.

## Supported Sinks:

Following sinks are supported in firehose

* [Log](https://en.wikipedia.org/wiki/Log_file)
* [HTTP](https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol)
* [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity)
* [InfluxDB](https://en.wikipedia.org/wiki/InfluxDB)
* [Redis](https://en.wikipedia.org/wiki/Redis)
* [ElasticSearch](https://en.wikipedia.org/wiki/Elasticsearch)
* [GRPC](https://en.wikipedia.org/wiki/GRPC)
* \[Prometheus\]\([https://en.wikipedia.org/wiki/Prometheus\_\(software](https://en.wikipedia.org/wiki/Prometheus_%28software)\)\) - `Coming soon`

## How is Firehose different from Kafka-connect?

* **Ease of use:** Firehose is easier to install and using different sink only requires chnaging few configurations. Kafka connect whhen used in distributed mode across multiple nodes, requires connectors to be installed across all the workers within your kafka connect cluster.
* **Filtering:** Value based filtering is much easier to implement as compared to Kafka-Connect. Requires no additional plugins/schema-registry to be installed.
* **Extensible:** Provides a comprehensible abstract sink contract which makes it easier to add a new sink in Firehose. Fireose also comes an inbuilt serialization/deserialization and doesn't require any converters and serializers when implementing new sink. 
* **Easy monitoring:** Firehose provides a detailed health dashboard \(Grafana\) for effortless monitoring.
* **Connectors:** Some of the Kafks connect available connectors usually have limitations. Its usually rare to find all the required features in a single connector and so is to find a documentation for the same
* **Fully open-source:** Firehose is completely open source while separation of commercial and open-source features is not very structured in Kafka Connect and for monitoring and advanced features confluent control center requires an enterprise subscription

## How can I get started?

Explore the following resources to get started with Firehose:

* [Guides](guides/overview.md) provides guidance on creating Firehose with different sinks.
* [Concepts](https://github.com/odpf/firehose/tree/e73c4e962d3c8a5b2306a7693a9e2a4b40c6e188/docs/concepts/overview.md) describes all important Firehose concepts.
* [Reference](https://github.com/odpf/firehose/tree/e73c4e962d3c8a5b2306a7693a9e2a4b40c6e188/docs/reference/README.md) contains details about configurations, metrics and other aspects of Firehose.
* [Contributing](https://github.com/odpf/firehose/tree/e73c4e962d3c8a5b2306a7693a9e2a4b40c6e188/docs/contribute/contributing.md) contains resources for anyone who wants to contribute to Firehose.

