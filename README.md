# Firehose
Firehose allows smooth and effortless consumption of data from Kafka. This data can then be used for different downstream applications like an HTTP service or database to create data driven applications, deliver crucial business insights in real time and monitor critical application flows.

Detailed documentation for Firehose can be found [here](). 
## Features
* FILTER: You can apply [filter]() on your input stream based on any field present in your ProtoBuf.
* HTTP Sink: Firehose supports HTTP sink with some exclusive features
    * Output in Protobuf format
    * [Templatized JSON Body]()
    * [Parameterised URL]() - JSON & Proto
    * [Parameterised Header]() - JSON & Proto

    For more details on these and more such features, Checkout the [Firehose HTTP Sink]()
* DB Sinks
    * [Postgres]()
    * [Redis]()
    * [Influx]()
    * [Elasticsearch]()
* GRPC Sink: Firehose supports [GRPC Sink]()
* Easy Monitoring: Exposes critical metrics to monitor the health of your Jobs. More details [here]()
* Retry: You can opt for retrying the failed messages. More details [here]()
* DLQ: Failed messages are pushed to a DLQ after certain number of retry attempts. More details [here]()
* Multi Threaded: You can increase the number of consumer threads. More details [here]()
* Easy Installation: Firehose can be easily deployed on VMs or Kubernetes clusters.
* Easy to Extend: Please check the [contributing guidelines]()
* Firehose is Horizontally scalable, has Docker & Helm Support

To know more about these and many more such features, checkout [Firehose Documentation]() 

## Build and Run it
* Clone the repo `git clone https://github.com/odpf/firehose.git`
* Build the jar `./gradlew clean build`
* Configure the environment variables in `env/local.properties`
* Run the firehose `./gradlew runConsumer`
####Sample Configuration for Log Sink Firehose
* To run firehose in any of the available sinks, the following variables need to be set

|        Name         |                              Description                              |          Example          |
| :-----------------: | :-------------------------------------------------------------------: | :-----------------------: |
|   `KAFKA_ADDRESS`   | The url/IP of the Kafka broker to which this consumer should connect to. |   `127.0.0.1:6667`   |
| `CONSUMER_GROUP_ID` |                     The Kafka consumer group id.                      | `kafka-consumer-group-id` |
|    `KAFKA_TOPIC`    |         The Kafka topic(s) this consumer should subscribe to.         |      `sample-topic`       |
|       `SINK`        |                      Sink mode for the firehose                       |          `log`            |
|   `PROTO_SCHEMA`    |              Fully qualified name of the proto schema for log message | `com.github.firehose.sampleLogProto.SampleLogMessage`  |

* The value of `KAFKA_TOPIC` variable could even be a regex. e.g. `sample-topic-1|sample-topic-2`.
* To subscribe to all smaple topics, the value of this config could be set to `sample-topic.*`.

* Sample configuration for other sinks along with some advanced configurations can be found [here]()

## Run unit tests
`./gradlew test`

## Run code quality checks
`./gradlew checkstyleMain checkstyleTest`

## Cleaning the build
`./gradlew clean`

## Run with Docker
* Firehose Docker image can be found [here]()
* Command to run simple Log Sink
```
docker run -e KAFKA_ADDRESS=127.0.0.1:6667 -e CONSUMER_GROUP_ID=kafka-consumer-group-id -e KAFKA_TOPIC=sample-topic -e SINK=log -e KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET=latest -e PROTO_SCHEMA=com.github.firehose.sampleLogProto.SampleLogMessage [FIREHOSE-DOCKER-IMAGE-URL] "java -cp bin/*:/work-dir/* com.gojek.esb.launch.Main"
```
NOTE: Make sure your protos (.jar file) are located in `work-dir`, this is required for Filter functionality to work.

## Run with Kubernetes
* Create a firehose deployment using the helm chart available [here]()
* Deployment also includes telegraf container which pushes stats metrics


## Fork and Contribute
This is an active open-source project. We are always open to people who want to use the system or contribute to it. You can raise a PR for any feature/possible bugs or help us with documentation.

To contribute follow the instructions here:
* url-to-main-documentation

Reach out to us on our mailing list <mailing-list>.
