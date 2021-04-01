# Firehose

![build workflow](https://github.com/odpf/firehose/actions/workflows/build.yml/badge.svg) ![package workflow](https://github.com/odpf/firehose/actions/workflows/package.yml/badge.svg)

Firehose is a cloud native service for delivering real-time streaming data to destinations such as service endpoints \(HTTP or GRPC\) & managed databases \(Postgres, InfluxDB, Redis, & Elasticsearch\). With Firehose, you don't need to write applications or manage resources. It can be scaled up to match the throughput of your data. If your data is present in Kafka, Firehose delivers it to the destination\(SINK\) that you specified.

## Key Features

Discover why users choose Firehose as their main Kafka Consumer

* **Sinks:** Firehose supports sinking stream data to log console, HTTP, GRPC, PostgresDB\(JDBC\), InfluxDB, Elasticsearch & Redis.
* **Scale:** Firehose scales in an instant, both vertically and horizontally  for high performance streaming sink and zero data drops.
* **Extensibility:** Add your own sink to firehose with a clearly defined interface or choose from already provided ones.
* **Runtime:** Firehose can run inside VMs or containers in a fully managed runtime environment like kubernetes.
* **Metrics:** Always know what’s going on with your deployment with built-in [monitoring](https://github.com/odpf/firehose/tree/e73c4e962d3c8a5b2306a7693a9e2a4b40c6e188/docs/assets/firehose-grafana-dashboard.json) of throughput, response times, errors and more.

To know more, follow the detailed [documentation](https://github.com/odpf/firehose/tree/e73c4e962d3c8a5b2306a7693a9e2a4b40c6e188/docs/README.md)

## Usage

Explore the following resources to get started with Firehose:

* [Guides](https://github.com/odpf/firehose/tree/e73c4e962d3c8a5b2306a7693a9e2a4b40c6e188/docs/guides/README.md) provides guidance on [creating Firehose](docs/guides/overview.md) with different sinks.
* [Concepts](https://github.com/odpf/firehose/tree/e73c4e962d3c8a5b2306a7693a9e2a4b40c6e188/docs/concepts/README.md) describes all important Firehose concepts.
* [Reference](https://github.com/odpf/firehose/tree/e73c4e962d3c8a5b2306a7693a9e2a4b40c6e188/docs/reference/README.md) contains details about configurations, metrics and other aspects of Firehose.
* [Contribute](docs/contribute/contribution.md) contains resources for anyone who wants to contribute to Firehose.

## Run with Docker

Use the docker hub to download firehose [docker image](https://hub.docker.com/r/odpf/firehose/). You need to have docker installed in your system.

```text
$ docker pull odpf/firehose
# Run the following docker command for a simple log sink.
$ docker run -e SOURCE_KAFKA_BROKERS=127.0.0.1:6667 -e SOURCE_KAFKA_CONSUMER_GROUP_ID=kafka-consumer-group-id -e SOURCE_KAFKA_TOPIC=sample-topic -e SINK_TYPE=log -e SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET=latest -e PROTO_SCHEMA=com.github.firehose.sampleLogProto.SampleLogMessage odpf/firehose:latest
```

**Note:** Make sure your protos \(.jar file\) are located in `work-dir`, this is required for Filter functionality to work.

## Run with Kubernetes

* Create a firehose deployment using the helm chart available [here](https://github.com/odpf/charts/tree/main/stable/firehose)
* Deployment also includes telegraf container which pushes stats metrics

## Running locally

```bash
$ git clone https://github.com/odpf/firehose.git  # Clone the repo
$ ./gradlew clean build # Build the jar
$ cat env/local.properties # Configure env variables
$ ./gradlew runConsumer # Run the firehose
```

**Note:** Sample configuration for other sinks along with some advanced configurations can be found [here](docs/reference/configuration.md)

## Running tests

```bash
# Running unit tests
./gradlew test

# Run code quality checks
./gradlew checkstyleMain checkstyleTest

#Cleaning the build
./gradlew clean
```

## Contribute

Development of Firehose happens in the open on GitHub, and we are grateful to the community for contributing bugfixes and improvements. Read below to learn how you can take part in improving Firehose.

Read our [contributing guide](docs/contribute/contribution.md) to learn about our development process, how to propose bugfixes and improvements, and how to build and test your changes to Firehose.

To help you get your feet wet and get you familiar with our contribution process, we have a list of [good first issues](https://github.com/odpf/firehose/labels/good%20first%20issue) that contain bugs which have a relatively limited scope. This is a great place to get started.

## Credits

This project exists thanks to all the [contributors](https://github.com/odpf/firehose/graphs/contributors).

## License

Firehose is [Apache 2.0](https://github.com/odpf/firehose/tree/e73c4e962d3c8a5b2306a7693a9e2a4b40c6e188/LICENSE/README.md) licensed.

