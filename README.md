# Firehose
![build workflow](https://github.com/odpf/firehose/actions/workflows/build.yml/badge.svg)
![publish workflow](https://github.com/odpf/firehose/actions/workflows/publish.yml/badge.svg)

Firehose is a cloud native service for delivering real-time streaming data to destinations such as service endpoints (HTTP or GRPC) & managed databases (Postgres, InfluxDB,  Redis, & ElasticSearch). With Firehose, you don't need to write applications or manage resources. It automatically scales to match the throughput of your data and requires no ongoing administration. If your data is present in Kafka, Firehose delivers it to the destination(SINK) that you specified.

<p align="center"><img src="./docs/assets/overview.svg" /></p>

## Key Features
Discover why users choose Firehose as their main Kafka Consumer

* **Support for multiple sinks** Firehose supports sinking stream data to log console, HTTP, GRPC, PostgresDB(JDBC), InfluxDB, ElasticSearch & Redis
* **Self-serve** Configure, deploy, validate, scale, monitor, alert, debug, audit and so on. Every action is DIY
* **Elastic scaling** Cloud-native & leverages horizontal scaling and high-performance streaming to sink data in near real-time & zero data drops
* **Metrics for performance monitoring** Exposes critical metrics through the consoles to monitor the health of your delivery streams, take any necessary actions
* **Easy Deployment:** Firehose can be easily deployed on VMs or Kubernetes clusters.

To know more, follow the detailed [documentation](docs) 

## Supported Sinks:
Following sinks are supported in firehose
* [Log](docs/guides/overview.md#create-a-log-sink)
* [HTTP](docs/guides/overview.md#create-an-http-sink)
* [JDBC](docs/guides/overview.md#create-a-jdbc-sink)
* [InfluxDB](docs/guides/overview.md#create-an-influxdb-sink)
* [Redis](docs/guides/overview.md#create-a-redis-sink)
* [ElasticSearch](docs/guides/overview.md#create-an-elasticsearch-sink)
* [GRPC](docs/guides/overview.md#create-a-grpc-sink)
* [Prometheus](https://prometheus.io/docs/introduction/overview/) - `Coming soon`

## How is Firehose different from Kafka-connect?
* **Ease of use:** Firehose is easier to install and using different sink only requires chnaging few configurations. Kafka connect whhen used in distributed mode across multiple nodes, requires connectors to be installed across all the workers within your kafka connect cluster.
* **Filtering:** Value based filtering is much easier to implement as compared to Kafka-Connect. Requires no additional plugins/schema-registry to be installed.
* **Extensible:** Provides a comprehensible abstract sink contract which makes it easier to add a new sink in Firehose. Fireose also comes with an inbuilt serialization/deserialization and doesn't require any converters and serializers when implementing new sink. 
* **Easy monitoring:** Firehose provides a detailed health dashboard (Grafana) for effortless monitoring.
* **Connectors:** Some of the Kafka connect available connectors usually have limitations. Its usually rare to find all the required features in a single connector and so is to find a documentation for the same.
* **Fully open-source:** Firehose is completely open source while separation of commercial and open-source features is not very structured in Kafka Connect and for monitoring and advanced features confluent control center requires an enterprise subscription.

## How can I get started?

Explore the following resources to get started with Firehose:

* [Guides](docs/guides) provides guidance on [creating Firehose](docs/guides/overview.md) with different sinks and [managing](docs/guides/manage.md) them effectively.
* [Concepts](docs/concepts) describes all important Firehose concepts.
* [Reference](docs/reference) contains details about configurations, metrics and other aspects of Firehose.
* [Contribute](docs/contribute/contribution.md) contains resources for anyone who wants to contribute to Firehose.

## Run with Docker
* Firehose Docker image is available on [ODPF docker hub](https://hub.docker.com/r/odpf/firehose/)
* Command to run simple Log Sink
```
docker run -e SOURCE_KAFKA_BROKERS=127.0.0.1:6667 -e SOURCE_KAFKA_CONSUMER_GROUP_ID=kafka-consumer-group-id -e SOURCE_KAFKA_TOPIC=sample-topic -e SINK_TYPE=log -e SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET=latest -e PROTO_SCHEMA=com.github.firehose.sampleLogProto.SampleLogMessage odpf/firehose:latest
```
**Note:** Make sure your protos (.jar file) are located in `work-dir`, this is required for Filter functionality to work.

## Run with Kubernetes
* Create a firehose deployment using the helm chart available [here]() - `Coming soon`
* Deployment also includes telegraf container which pushes stats metrics

## Running locally
* Clone the repo `git clone https://github.com/odpf/firehose.git`
* Build the jar `./gradlew clean build`
* Configure the environment variables in `env/local.properties`
* Sample configuration for other sinks along with some advanced configurations can be found [here](/docs/reference/configuration.md)
* Run the firehose `./gradlew runConsumer` 

## Running tests 
```sh
# Running unit tests
./gradlew test

# Run code quality checks
./gradlew checkstyleMain checkstyleTest

#Cleaning the build
./gradlew clean

```

## Contribute

Development of Firehose happens in the open on GitHub, and we are grateful to the community for contributing bugfixes and improvements. Read below to learn how you can take part in improving Firehose.

- Read our [contributing guide](docs/contribute/contribution.md) to learn about our development process, how to propose bugfixes and improvements, and how to build and test your changes to Jest.
- To help you get your feet wet and get you familiar with our contribution process, we have a list of [good first issues](https://github.com/odpf/firehose/labels/good%20first%20issue) that contain bugs which have a relatively limited scope. This is a great place to get started.

## Credits

This project exists thanks to all the people who [contribute](docs/contribute/contribution.md).

<a href="https://github.com/odpf/firehose/graphs/contributors"><img src="https://opencollective.com/firehose/contributors.svg?width=890&button=false" /></a>

## License
Firehose is [Apache 2.0](LICENSE) licensed.
