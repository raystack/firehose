## Run Firehose Locally 

- The following guide/walk through provides a simple way to run firehose locally, for log sink as an example.
- The guide uses the TestMessage (src/test/proto/TestMessage.proto) proto schema, which has already been provided for testing purposes.

## Configuring env/local.properties file

 Set the generic variables in the local.properties file.

```text
KAFKA_RECORD_PARSER_MODE = message
SINK_TYPE = log
INPUT_SCHEMA_PROTO_CLASS = io.odpf.firehose.consumer.TestMessage
```
 Set the variables which specify the kafka server, topic name, and group-id of the kafka consumer - the standard values are used here.
```text
SOURCE_KAFKA_BROKERS = localhost:9092
SOURCE_KAFKA_TOPIC = test-topic
SOURCE_KAFKA_CONSUMER_GROUP_ID = sample-group-id
```

## Stencil Workaround
 Firehose uses [Stencil](https://github.com/odpf/stencil) as the schema-registry which enables dynamic proto schemas. For the sake of this
 quick-setup guide, we can work our way around Stencil setup by setting up a simple local HTTP server which can provide the static descriptor for TestMessage schema.


 - Install a server service - like [this](https://github.com/http-party/http-server) one.
 
 - Generate the descriptor for TestMessage by running the command on terminal -
```shell
./gradlew generateTestProto
```
- The above should generate a file (src/test/resources/__files/descriptors.bin), move this to a new folder at a separate location, and start the HTTP-server there so that this file can be fetched at the runtime. 
- If you are using [this](https://github.com/http-party/http-server), use this command after moving the file to start server at the default port number 8080.
```shell
http-server
```
- Because we are not using the schema-registry in the default mode, the following lines should also be added in env/local.properties to specify the new location to fetch descriptor from.
```text
SCHEMA_REGISTRY_STENCIL_ENABLE = true
SCHEMA_REGISTRY_STENCIL_URLS = http://localhost:8080/descriptors.bin
SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH = false
SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY = LONG_POLLING
```

## Run Firehose Log Consumer

- Make sure that your kafka server and local HTTP server containing the descriptor is up and running.
- Run the firehose consumer through the gradlew task:
```shell
./gradlew runConsumer
```