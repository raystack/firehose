# Creating Firehose

This page contains how-to guides for creating Firehose with different sinks along with their features.

{% hint style="info" %}
If you'd like to connect to a sink which is not yet supported, you can create a new sink by following the [contribution guidelines](../contribute/contribution.md)
{% endhint %}

## Create a Log Sink

Firehose provides a log sink to make it easy to consume messages in [standard output](https://en.wikipedia.org/wiki/Standard_streams#Standard_output_%28stdout%29). A log sink firehose requires the following [variables](../reference/configuration.md#-generic) to be set. Firehose log sink can work in key as well as message parsing mode configured through [`KAFKA_RECORD_PARSER_MODE`](../reference/configuration.md#kafka_record_parser_mode)

An example log sink configurations:

```text
SOURCE_KAFKA_BROKERS=localhost:9092
SOURCE_KAFKA_TOPIC=test-topic
KAFKA_RECOED_CONSUMER_GROUP_ID=sample-group-id
KAFKA_RECORD_PARSER_MODE=message
SINK_TYPE=log
INPUT_SCHEMA_PROTO_CLASS=com.tests.TestMessage
```

Sample output of a Firehose log sink:

```text
2021-03-29T08:43:05,998Z [pool-2-thread-1] INFO  i.o.firehose.Consumer- Execution successful for 1 records
2021-03-29T08:43:06,246Z [pool-2-thread-1] INFO  i.o.firehose.Consumer - Pulled 1 messages
2021-03-29T08:43:06,246Z [pool-2-thread-1] INFO  io.odpf.firehose.sink.log.LogSink - 
================= DATA =======================
sample_field: 81179979
sample_field_2: 9897987987
event_timestamp {
  seconds: 1617007385
  nanos: 964581040
}
```

## Create an HTTP Sink

Firehose [HTTP](https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol) sink allows users to read data from Kafka and write to an HTTP endpoint. it requires the following [variables](../reference/configuration.md#-http-sink) to be set. You need to create your own HTTP endpoint so that the Firehose can send data to it.

### Supported Methods

Firehose supports `PUT` and `POST` verbs in its HTTP sink. The method can be configured using [`SINK_HTTP_REQUEST_METHOD`](../reference/configuration.md#-sink_http_request_method).

### Authentication

Firehose HTTP sink supports [OAuth](https://en.wikipedia.org/wiki/OAuth) authentication. OAuth can be enabled for the HTTP sink by setting [`SINK_HTTP_OAUTH2_ENABLE`](../reference/configuration.md#-sink_http_oauth2_enable)

```text
SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL: https://sample-oauth.my-api.com/oauth2/token  # OAuth2 Token Endpoint.
SINK_HTTP_OAUTH2_CLIENT_NAME: client-name  # OAuth2 identifier issued to the client.
SINK_HTTP_OAUTH2_CLIENT_SECRET: client-secret # OAuth2 secret issued for the client. 
SINK_HTTP_OAUTH2_SCOPE: User:read, sys:info  # Space-delimited scope overrides.
```

### Retries

Firehose allows for retrying to sink messages in case of failure of HTTP service. The HTTP error code ranges to retry can be configured with [`SINK_HTTP_RETRY_STATUS_CODE_RANGES`](../reference/configuration.md#-sink_http_retry_status_code_ranges). HTTP request timeout can be configured with [`SINK_HTTP_REQUEST_TIMEOUT_MS`](../reference/configuration.md#-sink_http_request_timeout_ms)

### Templating

Firehose HTTP sink supports payload templating using [`SINK_HTTP_JSON_BODY_TEMPLATE`](../reference/configuration.md#-sink_http_json_body_template) configuration. It uses [JsonPath](https://github.com/json-path/JsonPath) for creating Templates which is a DSL for basic JSON parsing. Playground for this: [https://jsonpath.com/](https://jsonpath.com/), where users can play around with a given JSON to extract out the elements as required and validate the `jsonpath`. The template works only when the output data format [`SINK_HTTP_DATA_FORMAT`](../reference/configuration.md#-sink_http_data_format) is JSON.

_**Creating Templates:**_

This is really simple. Find the paths you need to extract using the JSON path. Create a valid JSON template with the static field names + the paths that need to extract. \(Paths name starts with $.\). Firehose will simply replace the paths with the actual data in the path of the message accordingly. Paths can also be used on keys, but be careful that the element in the key must be a string data type.

One sample configuration\(On XYZ proto\) : `{"test":"$.routes[0]", "$.order_number" : "xxx"}` If you want to dump the entire JSON as it is in the backend, use `"$._all_"` as a path.

Limitations:

* Works when the input DATA TYPE is a protobuf, not a JSON.
* Supports only on messages, not keys.
* validation on the level of valid JSON template. But after data has been replaced the resulting string may or may not be a valid JSON. Users must do proper testing/validation from the service side.
* If selecting fields from complex data types like repeated/messages/map of proto, the user must do filtering based first as selecting a field that does not exist would fail.

## Create a JDBC SINK

* Supports only PostgresDB as of now.
* Data read from Kafka is written to the PostgresDB database and it requires the following [variables](../reference/configuration.md#-jdbc-sink) to be set.

_**Note: Schema \(Table, Columns, and Any Constraints\) being used in firehose configuration must exist in the Database already.**_

## Create an InfluxDB Sink

* Data read from Kafka is written to the InfluxDB time-series database and it requires the following [variables](../reference/configuration.md#-influx-sink) to be set.

_**Note:**_ [_**DATABASE**_](../reference/configuration.md#-sink_influx_db_name) _**and**_ [_**RETENTION POLICY**_](../reference/configuration.md#-sink_influx_retention_policy) _**being used in firehose configuration must exist already in the Influx, It’s outside the scope of a firehose and won’t be generated automatically.**_

## Create a Redis Sink

* it requires the following [variables](../reference/configuration.md#-redis-sink) to be set.
* Redis sink can be created in 2 different modes based on the value of [`SINK_REDIS_DATA_TYPE`](../reference/configuration.md#-sink_redis_data_type): HashSet or List
  * `Hashset`: For each message, an entry of the format `key : field : value` is generated and pushed to Redis. field and value are generated on the basis of the config [`INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING`](https://github.com/odpf/firehose/blob/documentation/docs/reference/configuration.md#-input_schema_proto_to_column_mapping-2)
  * `List`: For each message entry of the format `key : value` is generated and pushed to Redis. Value is fetched for the proto index provided in the config [`SINK_REDIS_LIST_DATA_PROTO_INDEX`](../reference/configuration.md#-sink_redis_list_data_proto_index)
* The `key` is picked up from a field in the message itself.
* Redis sink also supports different [Deployment Types](../reference/configuration.md#-sink_redis_deployment_type) `Standalone` and `Cluster`.
* Limitation: Firehose Redis sink only supports HashSet and List entries as of now.

## Create an Elasticsearch Sink

* it requires the following [variables](../reference/configuration.md#-elasticsearch-sink) to be set.
* In the Elasticsearch sink, each message is converted into a document in the specified index with the Document type and ID as specified by the user.
* Elasticsearch sink supports reading messages in both JSON and Protobuf formats.
* Using [Routing Key](../reference/configuration.md#-sink_es_routing_key_name) one can route documents to a particular shard in Elasticsearch.

## Create a GRPC Sink

* Data read from Kafka is written to a GRPC endpoint and it requires the following [variables](../reference/configuration.md#-grpc-sink) to be set.
* You need to create your own GRPC endpoint so that the Firehose can send data to it. The response proto should have a field “success” with value as true or false.

## Define Standard Configurations

* These are the configurations that remain common across all the Sink Types.
* You don’t need to modify them necessarily, It is recommended to use them with the default values. More details [here](../reference/configuration.md#-standard).

## 

