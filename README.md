# Firehose
Firehose allows smooth and effortless consumption of data from Kafka. This data can then be used for different downstream applications like an HTTP service or database to create data driven applications, deliver crucial business insights in real time and monitor critical application flows.

<p align="center"><img src="./docs/assets/overview.svg" /></p>

## Key Features
* **Sinks:** Firehose supports multipls sinks inlcuding HTTP, GRPC, JDBC, Redis, Elastic Search, Influx and more. 
* **Filters:** Firehose allows applying [filters]() on the input stream based on any field in the suported schema.
* **Monitoring:** Exposes critical [metrics]() to monitor the health of the running deployment.
* **Retries:** Firehose allows for retrying the messages which get failed to push to the sinks. 
* **DLQ:** Failed messages are pushed to a [DLQ]() after certain number of retry attempts. More details [here]()
* **Multi Threaded:** You can increase the number of consumer threads. More details [here]()
* **Easy Deployment:** Firehose can be easily deployed on VMs or Kubernetes clusters.
* **Scale:** Firehose is Horizontally scalable, has Docker & Helm Support 

To know more, follow the detailed [documentation]() 

## Run with Docker
* Firehose Docker image can be found [here]()
* Command to run simple Log Sink
```
docker run -e source.kafka.brokers=127.0.0.1:6667 -e source.kafka.consumer.group.id=kafka-consumer-group-id -e source.kafka.topic=sample-topic -e sink.type=log -e source.kafka.consumer.config.auto.offset.reset=latest -e proto.schema=com.github.firehose.sampleLogProto.SampleLogMessage odpf/firehose:latest
```
NOTE: Make sure your protos (.jar file) are located in `work-dir`, this is required for Filter functionality to work.

## Run with Kubernetes
* Create a firehose deployment using the helm chart available [here]()
* Deployment also includes telegraf container which pushes stats metrics

## Running locally
* Clone the repo `git clone https://github.com/odpf/firehose.git`
* Build the jar `./gradlew clean build`
* Configure the environment variables in `env/local.properties`
* Run the firehose `./gradlew runConsumer` 
#### Sample Configuration for Log Sink Firehose
* To run firehose in any of the available sinks, the following variables need to be set

```
source.kafka.brokers                # The url/IP of the Kafka broker to which this consumer should connect to
source.kafka.consumer.group.id      # The Kafka consumer group id
source.kafka.topic                  # The Kafka topic(s) this consumer should subscribe to
sink.type                           # Sink mode for the firehose
proto.schema                        # Fully qualified name of the proto schema for log message
```

* The value of `source.kafka.topic` variable could even be a regex. e.g. `sample-topic-1|sample-topic-2`.
* To subscribe to all smaple topics, the value of this config could be set to `sample-topic.*`.
* Sample configuration for other sinks along with some advanced configurations can be found [here]()

## Running tests 
```sh
# Running unit tests
`./gradlew test`

# Run code quality checks
`./gradlew checkstyleMain checkstyleTest`

#Cleaning the build
`./gradlew clean`

```

## Contribute
This is an active open-source project. We are always open to people who want to use the system or contribute to it. You can raise a PR for any feature/possible bugs or help us with documentation. To contribute follow the instructions [here]()

Reach out to us on our mailing list <mailing-list>.

## Contributing

Development of Firehose happens in the open on GitHub, and we are grateful to the community for contributing bugfixes and improvements. Read below to learn how you can take part in improving Firehose.

- Read our [contributing guide](CONTRIBUTING.md) to learn about our development process, how to propose bugfixes and improvements, and how to build and test your changes to Jest.
- To help you get your feet wet and get you familiar with our contribution process, we have a list of [good first issues](https://github.com/odpf/firehose/labels/good%20first%20issue) that contain bugs which have a relatively limited scope. This is a great place to get started.

## Credits

This project exists thanks to all the people who [contribute](CONTRIBUTING.md).

<a href="https://github.com/odpf/firehose/graphs/contributors"><img src="https://opencollective.com/firehose/contributors.svg?width=890&button=false" /></a>

## License
Firehose is Apache 2.0 licensed.