#### Introduction
Firehose is a fully managed service for delivering real-time streaming data to destinations such as service endpoints (HTTP or GRPC) & managed databases (Postgres, InfluxDB,  Redis, & ElasticSearch). With Firehose, you don't need to write applications or manage resources. It automatically scales to match the throughput of your data and requires no ongoing administration. If your data is present in Kafka, Firehose delivers it to the destination(SINK) that you specified.

Firehose can work with following sink modes 
* Log
* HTTP
* JDBC
* InfluxDB
* Redis
* ElasticSearch
* GRPC

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


## How to guides
#### Deploy your first Firehose
Coming Soon...

#### Create a Log Sink
**A log sink firehose requires the following variables to be set.**

| VARIABLE NAME | DESCRIPTION | NECESSITY | SAMPLE VALUE |
|---|---|---|---|
| source.kafka.brokers | list of kafka brokers to consume from  | Mandatory | localhost:9092 |
| source.kafka.topic | list of kafka topics to consume from  | Mandatory | test-topic |
| source.kafka.consumer.group.id | Kafka consumer group ID | Mandatory | sample-group-id |
| kafka.record.parcer.mode | Decides whether to parse key or message | Mandatory | message |
| sink.type | Firehose sink type | Mandatory | log |
| input.schema.proto.class | The fully qualified name of the input proto class | Mandatory | com.tests.TestMessage |

#### Create an HTTP Sink
**Data read from Kafka is written to an HTTP endpoint and it requires the following variables to be set. You need to create your own HTTP endpoint so that the Firehose can send data to it.**

| VARIABLE NAME                                                       | DESCRIPTION                                                                                                                                                                                                                                                                                                                                                                                                                          | NECESSITY                                            | SAMPLE VALUE                                                                                                                                                                                                                             |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| sink.http.service.url                                               | The HTTP endpoint of the service to which this consumer should PUT/POST data. This can be configured as per the requirement, a constant or a dynamic one (which extract given field values from each message and use that as the endpoint)<br>If service url is constant, messages will be sent as batches while in case of dynamic one each message will be sent as a separate request (Since they’d be having different endpoints) | Mandatory                                            | http://http-service.test.io<br><br>http://http-service.test.io/test-field/%%s,6<br><br>(This will take the value with index 6 from proto and create the endpoint as per the template) NOTE: need to use %% instead of single % |
| sink.http.request.method                                            | HTTP verb supported by the endpoint, Supports PUT, and POST verbs as of now.<br>Default value: put                                                                                                                                                                                                                                                                                                                                   | Mandatory                                            | post                                                                                                                                                                                                                                     |
| sink.http.request.timeout.ms                                        | The connection timeout for the request.<br>Default value: 10000                                                                                                                                                                                                                                                                                                                                                                      | Mandatory                                            | 10000                                                                                                                                                                                                                                    |
| sink.http.max.connections                                           | The maximum number of HTTP connections.<br>Default value: 10                                                                                                                                                                                                                                                                                                                                                                         | Mandatory                                            | 10                                                                                                                                                                                                                                       |
| sink.http.retry.status.code.ranges                                  | The range of HTTP status codes for which retry will be attempted.<br>Default value: 400-600                                                                                                                                                                                                                                                                                                                                          | Optional                                             | 400-600                                                                                                                                                                                                                                  |
| sink.http.data.format                                               | If proto, the log message will be sent as Protobuf byte strings. Otherwise, the log message will be deserialized into readable JSON strings.<br>Default value: proto                                                                                                                                                                                                                                                                 | Mandatory                                            | JSON                                                                                                                                                                                                                                     |
| sink.http.headers                                                   | The HTTP headers required to push the data to the above URL.                                                                                                                                                                                                                                                                                                                                                                         | Optional                                             | Authorization:auth\_token, Accept:text/plain                                                                                                                                                                                             |
| sink.http.json.body.template                                        | A template for creating a custom request body from the fields of a protobuf message. This should be a valid JSON itself.                                                                                                                                                                                                                                                                                                             | Optional                                             | {"test":"$.routes\[0\]", "$.order\_number" : "xxx"}                                                                                                                                                                                      |
| sink.http.parameter.source                                          | The source from which the fields should be parsed. This field should be present in order to use this feature.<br>Parameterized sink-specific<br>Default value: None                                                                                                                                                                                                                                                                  | Optional                                             | None, Key, Message                                                                                                                                                                                                                       |
| sink.http.parameter.placement                                       | The fields parsed can be passed in query parameters or in headers.<br>Parameterized sink-specific                                                                                                                                                                                                                                                                                                                                    | Optional                                             | Header, Query                                                                                                                                                                                                                            |
| input.output.mapping                                                | The mapping of the proto fields to header/query fields in JSON format.<br>Parameterized sink-specific                                                                                                                                                                                                                                                                                                                                | Optional                                             | {"1":"order\_number","2":"event\_timestamp","3":"driver\_id"}                                                                                                                                                                            |
| sink.http.oauth2.enable                                             | Enable/Disable OAuth2 support for HTTP sink<br>Default value: false                                                                                                                                                                                                                                                                                                                                                                  | Optional                                             | FALSE                                                                                                                                                                                                                                    |
| sink.http.oauth2.access.token.url                                   | OAuth2 Token Endpoint.                                                                                                                                                                                                                                                                                                                                                                                                               | Mandatory<br>( when sink.http.oauth2.enable = true ) | https://sample-oauth.my-api.com/oauth2/token                                                                                                                            |
| sink.http.oauth2.client.name                                        | OAuth2 identifier issued to the client.                                                                                                                                                                                                                                                                                                                                                                                              | Mandatory<br>( when sink.http.oauth2.enable = true ) | clientid                                                                                                                                                                                                                                 |
| sink.http.oauth2.client.secret                                      | OAuth2 secret issued for the client.                                                                                                                                                                                                                                                                                                                                                                                                 | Mandatory<br>( when sink.http.oauth2.enable = true ) | my-secret                                                                                                                                                                                                                                |
| sink.http.oauth2.scope                                              | Space-delimited scope overrides. If scope override is not provided, no scopes will be granted to the token.                                                                                                                                                                                                                                                                                                                          | Mandatory<br>( when sink.http.oauth2.enable = true ) | User:read, sys:info                                                                                                                                                                                                                      |

**Usage of sink.http.json.body.template is explained here.** 

***Templating Body In Firehose***
We are using: https://github.com/json-path/JsonPath - for creating Templates which is a DSL for basic JSON parsing. Playground for this: https://jsonpath.com/, where users can play around with a given JSON to extract out the elements as required and validate the jsonpath. The template works only when the output data format `sink.http.data.format` is JSON.

***Creating Templates:***

This is really simple. Find the paths you need to extract using the JSON path. Create a valid JSON template with the static field names + the paths that need to extract. (Paths name starts with $.). Firehose will simply replace the paths with the actual data in the path of the message accordingly. Paths can also be used on keys, but be careful that the element in the key must be a string data type.

One sample configuration : {"test":"$.routes[0]", "$.order_number" : "xxx"} (On XYZ proto).
If you want to dump the entire JSON as it is in the backend, use "$._all_" as a path.

Limitations:
* Works when the input DATA TYPE is a protobuf, not a JSON.
* Supports only on messages, not keys.
* validation on the level of valid JSON template. But after data has been replaced the resulting string may or may not be a valid JSON. Users must do proper testing/validation from the service side.
* If selecting fields from complex data type like repeated/messages/map of proto, the user must do filtering based first as selecting a field that does not exist would fail.


#### Create a JDBC SINK
* Supports only PostgresDB as of now
* Data read from Kafka is written to the PostgresDB database and it requires the following variables to be set.

| VARIABLE NAME                             | DESCRIPTION                                                                                                                 | NECESSITY | SAMPLE VALUE                                                                                                                                                           |
| ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| sink.jdbc.url                             | PostgresDB URL, it's usually the hostname followed by port.                                                                 | Mandatory | jdbc:postgresql://localhost:5432/postgres                                                                                                                 |
| sink.jdbc.table.name                      | The name of the table in which the data should be dumped.                                                                   | Mandatory | public.customers                                                                                                                                                       |
| sink.jdbc.username                        | The username to connect to DB                                                                                               | Mandatory | root                                                                                                                                                                   |
| sink.jdbc.password                        | The password to connect to DB                                                                                               | Mandatory | root                                                                                                                                                                   |
| input.schema.proto.to.column.mapping      | The mapping of fields in DB and the corresponding proto index from where the value will be extracted. This is a JSON field. | Mandatory | {"6":"customer\_id","1":"service\_type","5":"event\_timestamp"}(Proto field value with index 1 will be stored in a column named service\_type in DB and so on)         |
| sink.jdbc.unique.keys                     | Comma-separated column names having a unique constraint on the table.<br>Default value: NONE                                    | Optional  | customer\_id                                                                                                                                                           |
| sink.jdbc.connection.pool.timeout.ms      | Database connection timeout in milliseconds.<br>Default value: 1000                                                             | Mandatory | 2000                                                                                                                                                                   |
| sink.jdbc.connection.pool.idle.timeout.ms | Database connection pool idle connection timeout in milliseconds.<br>Default value: 60000                                       | Mandatory | 30000                                                                                                                                                                  |
| sink.jdbc.connection.pool.min.idle        | The minimum number of idle connections in the pool to maintain.<br>Default value: 0                                             | Mandatory | 0                                                                                                                                                                      |
| sink.jdbc.connection.pool.max.size        | Maximum size for the database connection pool.<br>Default value: 10                                                           | Mandatory | 10                                                                                                                                                                     |

***Note: Schema (Table, Columns, and Any Constraints) being used in firehose configuration must exist in the Database already.***

#### Create an Influx Sink
* Data read from Kafka is written to the InfluxDB time-series database and it requires the following variables to be set.

| VARIABLE NAME                              | DESCRIPTION                                                                                                                                                                                                                                                                                                                                                                         | NECESSITY | SAMPLE VALUE                                                                                                                                                         |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| sink.influx.url                            | InfluxDB URL, it's usually the hostname followed by port.                                                                                                                                                                                                                                                                                                                           | Mandatory | http://localhost:8086                                                                                                                                                |
| sink.influx.username                       | The username to connect to DB                                                                                                                                                                                                                                                                                                                                                       | Mandatory | root                                                                                                                                                                 |
| sink.influx.password                       | The password to connect to DB                                                                                                                                                                                                                                                                                                                                                       | Mandatory | root                                                                                                                                                                 |
| sink.influx.field.name.proto.index.mapping | The mapping of fields and the corresponding proto index which can be used to extract the field value from the proto message. This is a JSON field. Note that Influx keeps a single value for each unique set of tags and timestamps. If a new value comes with the same tag and timestamp from the source, it will override the existing one.                                       | Mandatory | {"2":"order\_number","1":"service\_type", "4":"status"<br>(Proto field value with index 2 will be stored in a field named 'order\_numbert' in Influx and so on) |
| sink.influx.tag.name.proto.index.mapping   | The mapping of tags and the corresponding proto index from which the value for the tags can be obtained. If the tags contain existing fields from field name mapping it will not be overridden. They will be duplicated. If ENUMS are present then they must be added here. This is a JSON field.                                                                                   | Optional  | {"6":"customer\_id"}<br>                                                                                                                                           |
| sink.influx.proto.event.timestamp.index    | Proto index of a field that can be used as the timestamp                                                                                                                                                                                                                                                                                                                            | Mandatory | 5                                                                                                                                                                    |
| sink.influx.db.name                        | InfluxDB database name where data will be dumped                                                                                                                                                                                                                                                                                                                                    | Mandatory | status                                                                                                                                                               |
| sink.influx.measurement.name               | This field is used to give away the name of the measurement that needs to be used by the sink. Measurement is another name for tables and it will be auto-created if not exist at the time Firehose pushes the data to the influx.                                                                                                                                                  | Mandatory | customer-booking                                                                                                                                                     |
| sink.influx.retention.policy               | Retention policy for influx database.<br>Default value: autogen                                                                                                                                                                                                                                                                                                                     | Mandatory | quarterly                                                                                                                                                            |

***Note: DATABASE and RETENTION POLICY being used in firehose configuration must exist already in the Influx, It’s outside the scope of a firehose and won’t be generated automatically.***

#### Create a Redis Sink

* Redis sink can be created in 2 different modes based on the value of `sink.redis.data.type`: Hashset or List
    * Hashset: For each message, an entry of the format ‘key : field : value’ is generated and pushed to Redis. field and value are generated on the basis of the config `input.schema.proto.to.column.mapping`
    * List: For each message entry of the format ‘key : value’ is generated and pushed to Redis. Value is fetched for the proto index provided in the config `sink.redis.list.data.proto.index`
* The ‘key’ is picked up from a field in the message itself.
* Limitation: Firehose Redis sink only supports HashSet and List entries as of now.
* it requires the following variables to be set.

| VARIABLE NAME                    | DESCRIPTION                                                                                                                                                                                                                   | NECESSITY              | SAMPLE VALUE                                                                                                                                                                                                             |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| sink.redis.urls                  | REDIS instance hostname/IP address followed by its port                                                                                                                                                                       | Mandatory              | localhos:6379,localhost:6380                                                                                                                                                                                                             |
| sink.redis.data.type             | To select whether you want to push your data as a HashSet or as a List.Default value: LIST                                                                                                                                    | Mandatory              | List / Hashset                                                                                                                                                                                                             |
| sink.redis.key.template          | The string that will act as the key for each Redis entry. This key can be configured as per the requirement, a constant or can extract value from each message and use that as the Redis key. <br>                            | Mandatory              | Service\_%%s,1<br>(This will take the value with index 1  from proto and create the Redis keys as per the template)                                                                                                        |
| input.schema.proto.to.column.mapping| This is the field that decides what all data will be stored in the HashSet for each message.                                                                                                                               | Mandatory<br>(Hashset) | {"6":"customer\_id",  "2":"order\_num"}                                                                                                                                                                                 |
| sink.redis.list.data.proto.index | This field decides what all data will be stored in the List for each message. <br>                                                                                                                                            | Mandatory<br>(List)     | 6<br>(this will get the value of the field with index 6 in your proto and push that to the Redis list with the corresponding keyTemplate)                                                                                 |
| sink.redis.ttl.type              | Choice of Redis TTL type.It can be:<br>DURATION: After which the Key will be expired and removed from Redis (UNIT- seconds)<br>EXACT\_TIME: Precise UNIX timestamp after which the Key will be expired<br>Default value: DISABLE | Optional               | DISABLE / DURATION / EXACT\_TIME                                                                                                                                                                                                 |
| sink.redis.ttl.value             | Redis TTL value in Unix Timestamp for EXACT\_TIME TTL type, In Seconds for DURATION TTL type<br>Default value: 0                                                                                                                  | Optional               | 100000<br>                                                                                                                                                                                                                 |
| sink.redis.deployment.type       | The Redis deployment you are using. At present, we support standalone and cluster types.<br>Default value: Standalone                                                                                                             | Optional               | Cluster                                                                                                                                                                                                                 |

#### Crete a Redis Cluster Sink

* Just like Redis sink, Redis cluster sink can be created in 2 different modes based on the value of `sink.redis.data.type`: Hashset or List
    * Hashset: For each message, an entry of the format ‘key : field : value’ is generated and pushed to Redis. field and value are generated on the basis of the config `input.schema.proto.to.column.mapping`
    * List: For each message, an entry of the format ‘key : value’ is generated and pushed to Redis. Value is fetched for the proto index provided in the config `sink.redis.list.data.proto.index`
* The ‘key’ is picked up from a field in the message itself.
* Limitation: Firehose Redis sink only supports HashSet and List entries as of now.
* it requires the following variables to be set.

| VARIABLE NAME                    | DESCRIPTION                                                                                                                                                                                                                   | NECESSITY              | SAMPLE VALUE                                                                                                                                                                                                             |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| sink.redis.urls                  | REDIS instance hostname/IP address followed by its port                                                                                                                                                                       | Mandatory              | localhost:6379                                                                                                                                                                                                             |
| sink.redis.data.type             | To select whether you want to push your data as a HashSet or as a List.Default value: LIST                                                                                                                                    | Mandatory              | List / Hashset                                                                                                                                                                                                             |
| sink.redis.key.template          | The string that will act as the key for each Redis entry. This key can be configured as per the requirement, a constant or can extract value from each message and use that as the Redis key. <br>                            | Mandatory              | Service\_%%s,1<br>(This will take the value with index 1  from proto and create the Redis keys as per the template)                                                                                                        |
| input.schema.proto.to.column.mapping| This is the field that decides what all data will be stored in the HashSet for each message.                                                                                                                               | Mandatory<br>(Hashset) | {"6":"customer\_id",  "2":"order\_num"}                                                                                                                                                                                 |
| sink.redis.list.data.proto.index | This field decides what all data will be stored in the List for each message. <br>                                                                                                                                            | Mandatory<br>(List)     | 6<br>(this will get the value of the field with index 6 in your proto and push that to the Redis list with the corresponding keyTemplate)                                                                                 |
| sink.redis.ttl.type              | Choice of Redis TTL type.It can be:<br>DURATION: After which the Key will be expired and removed from Redis (UNIT- seconds)<br>EXACT\_TIME: Precise UNIX timestamp after which the Key will be expired<br>Default value: DISABLE | Optional               | DISABLE / DURATION / EXACT\_TIME                                                                                                                                                                                                 |
| sink.redis.ttl.value             | Redis TTL value in Unix Timestamp for EXACT\_TIME TTL type, In Seconds for DURATION TTL type<br>Default value: 0                                                                                                                  | Optional               | 100000<br>                                                                                                                                                                                                                 |
| sink.redis.deployment.type       | The Redis deployment you are using. At present, we support standalone and cluster types.<br>Default value: Standalone                                                                                                             | Optional               | Standalone                                                                                                                                                                                                                 |

#### Create an ElasticSearch Sink

* In the ElasticSearch sink, each message is converted into a document in the specified index with the Document type and ID as specified by the user
* ElasticSearch sink supports reading messages in both JSON and Protobuf formats.
* it requires the following variables to be set.

| VARIABLE NAME                             | DESCRIPTION                                                                                                                                                                                                                                                                                                                                                                  | NECESSITY | SAMPLE VALUE    |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | --------------- |
| sink.es.connection.urls                   | Elastic search connection URL to connect. URLs could be comma separated like host\_ip1:port1, host\_ip2:port2                                                                                                                                                                                                                                                                | Mandatory | localhost1:9200 |
| sink.es.index.name                        | The name of the index to which you want to write the documents. If it doesn’t exist, it will be created.                                                                                                                                                                                                                                                                     | Mandatory | sample\_index   |
| sink.es.type.name                         | The type name of the Document in ElasticSearch.                                                                                                                                                                                                                                                                                                                              | Mandatory | Customer        |
| sink.es.id.field                          | The identifier field of the document in ElasticSearch. This should be the key of the field present in the message (JSON or Protobuf) and it has to be a unique, non-null field. So the value of this field in the message will be used as the ID of the document in ElasticSearch while writing the document.                                                                | Mandatory | customer\_id    |
| sink.es.mode.update.only.enable           | Elasticsearch sink can be created in 2 modes: Upsert mode or UpdateOnly mode<br>If this config is set to-<br>TRUE: Firehose will run on UpdateOnly mode which will only UPDATE the already existing documents in the ElasticSearch index.<br>FALSE: Firehose will run on Upsert mode, UPDATING the existing documents and also INSERTING any new ones<br>Default value: FALSE| Mandatory | FALSE           |
| sink.es.input.message.type                | Indicates if the Kafka topic contains JSON or Protocol Buffer messages.<br>Default value: JSON                                                                                                                                                                                                                                                                               | Mandatory | PROTOBUF        |
| sink.es.preserve.proto.field.names.enable | Whether or not the protobuf field names should be preserved in the ElasticSearch document. If false the fields will be converted to camel case.<br>Default value: TRUE                                                                                                                                                                                                             | Mandatory | FALSE           |
| sink.es.request.timeout.ms                | The request timeout of the elastic search endpoint. The value specified is in milliseconds<br>Default value: 60000                                                                                                                                                                                                                                                           | Mandatory | 60000           |
| sink.es.shards.active.wait.count          | The number of shard copies that must be active before proceeding with the operation. This can be set to any non-negative value less than or equal to the total number of shard copies (number of replicas + 1)<br>Default value: 1                                                                                                                                           | Mandatory | 1               |
| sink.es.retry.status.code.blacklist       | List of comma-separated status codes for which firehose should not retry in case of UPDATE ONLY mode is TRUE<br>Default value: 404                                                                                                                                                                                                                                           | Optional  | 404,400         |
| sink.es.routing.key.name                  | The proto field whose value will be used for routing documents to a particular shard in Elasticsearch. If empty, Elasticsearch uses the ID field of the doc by default                                                                                                                                                                                                       | Optional  | service\_type   |

#### Create a GRPC Sink

* Data read from Kafka is written to a GRPC endpoint and it requires the following variables to be set.
* You need to create your own GRPC endpoint so that the Firehose can send data to it. The response proto should have a field “success” with value as true or false.

| VARIABLE NAME                   | DESCRIPTION                                                | NECESSITY | SAMPLE VALUE                                                                                            |
| ------------------------------- | ---------------------------------------------------------- | --------- | ---------------------------------------- |
| sink.grpc.service.host          | The host of the GRPC service                               | Mandatory | http://grpc-service.sample.io, 127.0.0.1 |
| sink.grpc.service.port          | Port of the GRPC service                                   | Mandatory | 8500                                     |
| sink.grpc.method.url            | URL of the GRPC method that needs to be called             | Mandatory | com.tests.SampleServer/SomeMethod        |
| sink.grpc.response.proto.schema | Proto which would be the response of the GRPC Method       | Mandatory | Values derived from protos pre-defined   |

#### Define Standard Configurations

* These are the configurations that remain common across all the Sink Types.
* You don’t need to modify them necessarily, It is recommended to use them with the default values. Read their descriptions for more details.

| VARIABLE NAME                                 | DESCRIPTION                                                                                               | NECESSITY | SAMPLE VALUE |
| --------------------------------------------- | --------------------------------------------------------------------------------------------------------- | --------- | ------------ |
| source.kafka.consumer.config.max.poll.records | The maximum number of records, the consumer will fetch from Kafka in one request.<br>Default value: 500   | Mandatory | 500          |
| retry.exponential.backoff.initial.ms          | Initial expiry time in milliseconds for exponential backoff policy.<br>Default value: 10                  | Mandatory | 10           |
| retry.exponential.backoff.rate                | Backoff rate for exponential backoff policy.<br>Default value: 2                                          | Mandatory | 2            |
| retry.exponential.backoff.max.ms              | Maximum expiry time in milliseconds for exponential backoff policy.<br>Default value: 60000               | Mandatory | 60000        |

#### Filter Expressions
**Introduction**

Filter expressions are allowed to filter messages just after reading from Kafka and before sending to Sink.

##### Rules to write expressions:

* All the expressions are like a piece of Java code.
* Follow rules for every data type, as like writing a Java code.
* Access nested fields by `.` and `()`, i.e., `sampleLogMessage.getVehicleType()`

**Example**

Sample proto message:

```
===================KEY==========================
driver_id: "abcde12345"
vehicle_type: BIKE
event_timestamp {
  seconds: 186178
  nanos: 323080
}
driver_status: UNAVAILABLE

================= MESSAGE=======================
driver_id: "abcde12345"
vehicle_type: BIKE
event_timestamp {
  seconds: 186178
  nanos: 323080
}
driver_status: UNAVAILABLE
app_version: "1.0.0"
driver_location {
  latitude: 0.6487193703651428
  longitude: 0.791822075843811
  altitude_in_meters: 0.9949166178703308
  accuracy_in_meters: 0.39277541637420654
  speed_in_meters_per_second: 0.28804516792297363
}
gcm_key: "abc123"
```

***Key based filter expressions examples:***

* `sampleLogKey.getDriverId()=="abcde12345"`
* `sampleLogKey.getVehicleType()=="BIKE"`
* `sampleLogKey.getEventTimestamp().getSeconds()==186178`
* `sampleLogKey.getDriverId()=="abcde12345"&&sampleLogKey.getVehicleType=="BIKE"` (multiple conditions example 1)
* `sampleLogKey.getVehicleType()=="BIKE"||sampleLogKey.getEventTimestamp().getSeconds()==186178` (multiple conditions example 2)

***Message based filter expressions examples:***

* `sampleLogMessage.getGcmKey()=="abc123"`
* `sampleLogMessage.getDriverId()=="abcde12345"&&sampleLogMessage.getDriverLocation().getLatitude()>0.6487193703651428`
* `sampleLogMessage.getDriverLocation().getAltitudeInMeters>0.9949166178703308`

**Note: Use `sink.type=log` for testing the applied filtering** 

#### Manage consumer lag
* When it comes to decreasing the topic lag, it helps to have the `source.kafka.consumer.config.max.poll.records` config to be increased from the default of 500 to something higher.
* Additionally, you can increase the workers in the firehose which will effectively multiply the number of records being processed by firehose. However, please be mindful of the caveat mentioned below.
* The caveat to the aforementioned remedies: Be mindful of the fact that your sink also needs to be able to process this higher volume of data being pushed to it. Because if it is not, then this will only compound the problem of increasing lag.
* Alternatively, if your underlying sink is not able to handle increased (or default) volume of data being pushed to it, adding some sort of a filter condition in the firehose to ignore unnecessary messages in the topic would help you bring down the volume of data being processed by the sink.



## CONTRIBUTION GUIDELINES
#### BECOME A COMMITOR & CONTRIBUTE

We are always interested in adding new contributors. What we look for is a series of contributions, good taste, and an ongoing interest in the project.

* Committers will have write access to the Firehose repositories.
* There is no strict protocol for becoming a committer or PMC member. Candidates for new committers are typically people that are active contributors and community members.
* Candidates for new committers can also be suggested by current committers or PMC members.
* If you would like to become a committer, you should start contributing to Firehose in any of the ways mentioned. You might also want to talk to other committers and ask for their advice and guidance.


#### WHAT CAN YOU DO?
* You can report a bug or suggest a feature enhancement on our [Jira Board](link to create a new issue). If you have a question or are simply not sure if it is really an issue or not, please [contact us](E-mail) first before you create a new JIRA ticket.
* You can modify the code
    * Add any new feature
    * Add a new Sink
    * Improve Health and Monitoring Metrics
    * Improve Logging
* You can help with documenting new features or improve existing documentation.
* You can also review and accept other contributions if you are a Commitor.


#### GUIDELINES
Please follow these practices for you change to get merged fast and smoothly:

* Contributions can only be accepted if they contain appropriate testing (Unit and Integration Tests).
* If you are introducing a completely new feature or making any major changes in an existing one, we recommend to start with an RFC and get consensus on the basic design first.
* Make sure your local build is running with all the tests and checkstyle passing.
* If your change is related to user-facing protocols / configurations, you need to make the corresponding change in the documentation as well.
    * You may need to contact us to get the permission first if it is your first time to edit the documentation.
    * Docs live in the code repo under `docs` so that changes to that can be done in the same PR as changes to the code.
    
## Firehose VS Kafka-Connect
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