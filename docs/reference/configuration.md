# Configurations

This page contains reference for all the application configurations for Firehose.

### <a name="Generic" /> Generic
A log sink firehose requires the following variables to be set

#### <a name="source.kafka.brokers" /> `source.kafka.brokers` 

Example value: `localhost:9092`\
Type: `required`

Sets the bootstrap server of kafka brokers to consume from.

#### <a name="source.kafka.topic" /> `source.kafka.topic` 

Example value: `test-topic`\
Type: `required`

List of kafka topics to consume from    

#### <a name="source.kafka.consumer.group.id" /> `source.kafka.consumer.group.id` 

Example value: `sample-group-id`\
Type: `required`

Kafka consumer group ID .

#### <a name="kafka.record.parser.mode" /> `kafka.record.parser.mode` 

Example value: `message`\
Type: `required`

Decides whether to parse key or message

#### <a name="sink.type" /> `sink.type` 

Example value: `'localhost:9092'`\
Type: `required`

Firehose sink type 

#### <a name="input.schema.proto.class" /> `input.schema.proto.class` 

Example value: `com.tests.TestMessage`\
Type: `required`

The fully qualified name of the input proto class 

### <a name="HTTP Sink" /> HTTP Sink
An Http sink firehose requires the following variables to be set along with Generic ones

#### <a name="sink.http.service.url" /> `sink.http.service.url` 

Example value: `http://http-service.test.io`\
Example value: `http://http-service.test.io/test-field/%%s,6` This will take the value with index 6 from proto and create the endpoint as per the template\
Type: `required`

The HTTP endpoint of the service to which this consumer should PUT/POST data. This can be configured as per the requirement, a constant or a dynamic one (which extract given field values from each message and use that as the endpoint)<br>If service url is constant, messages will be sent as batches while in case of dynamic one each message will be sent as a separate request (Since they’d be having different endpoints)

#### <a name="sink.http.request.method" /> `sink.http.request.method`

Example value: `post`\
Type: `required`\
Default value: `put`

HTTP verb supported by the endpoint, Supports PUT, and POST verbs as of now.

#### <a name="sink.http.request.timeout.ms" /> `sink.http.request.timeout.ms`
Example value: `10000`\
Type: `required`\
Default value: `10000`

The connection timeout for the request in millis.

#### <a name="sink.http.max.connections" /> `sink.http.max.connections`
Example value: `10`\
Type: `required`\
Default value: `10`

The maximum number of HTTP connections.

#### <a name="sink.http.retry.status.code.ranges" /> `sink.http.retry.status.code.ranges`
Example value: `400-600`\
Type: `optional`\
Default value: `400-600`

The range of HTTP status codes for which retry will be attempted.

#### <a name="sink.http.data.format" /> `sink.http.data.format`
Example value: `JSON`\
Type: `required`\
Default value: `proto`

If proto, the log message will be sent as Protobuf byte strings. Otherwise, the log message will be deserialized into readable JSON strings.

#### <a name="sink.http.headers" /> `sink.http.headers`
Example value: `Authorization:auth_token, Accept:text/plain`\
Type: `optional`

The HTTP headers required to push the data to the above URL.

#### <a name="sink.http.json.body.template" /> `sink.http.json.body.template`
Example value: `{"test":"$.routes[0]", "$.order_number" : "xxx"}`\
Type: `optional`

A template for creating a custom request body from the fields of a protobuf message. This should be a valid JSON itself.	

#### <a name="sink.http.parameter.source" /> `sink.http.parameter.source`
Example value: `Key`\
Example value: `Message`\
Type: `optional`\
Default value: `None`

The source from which the fields should be parsed. This field should be present in order to use this feature.

#### <a name="sink.http.parameter.placement" /> `sink.http.parameter.placement`
Example value: `Header`\
Example value: `Query`\
Type: `optional`

The fields parsed can be passed in query parameters or in headers.

#### <a name="sink.http.parameter.schema.proto.class" /> `sink.http.parameter.schema.proto.class`
Example value: `com.tests.TestMessage`\
Type: `optional`

The fully qualified name of the proto class which is to be used for parametrised http sink.

#### <a name="input.schema.proto.to.column.mapping" /> `input.schema.proto.to.column.mapping`
Example value: `{"1":"order_number","2":"event_timestamp","3":"driver_id"}`\
Type: `optional`

The mapping of the proto fields to header/query fields in JSON format.

#### <a name="sink.http.oauth2.enable" /> `sink.http.oauth2.enable`
Example value: `true`\
Type: `optional`\
Default value: `false`

Enable/Disable OAuth2 support for HTTP sink.

#### <a name="sink.http.oauth2.access.token.url" /> `sink.http.oauth2.access.token.url`
Example value: `https://sample-oauth.my-api.com/oauth2/token`\
Type: `optional`

OAuth2 Token Endpoint.

#### <a name="sink.http.oauth2.client.name" /> `sink.http.oauth2.client.name`
Example value: `client-name`\
Type: `optional`

OAuth2 identifier issued to the client.

#### <a name="sink.http.oauth2.client.secret" /> `sink.http.oauth2.client.secret`
Example value: `client-secret`\
Type: `optional`

OAuth2 secret issued for the client.

#### <a name="sink.http.oauth2.scope" /> `sink.http.oauth2.scope`
Example value: `User:read, sys:info`\
Type: `optional`

Space-delimited scope overrides. If scope override is not provided, no scopes will be granted to the token.

### <a name="JDBC Sink" /> JDBC Sink
A JDBC sink firehose requires the following variables to be set along with Generic ones

#### <a name="sink.jdbc.url" /> `sink.jdbc.url`
Example value: `jdbc:postgresql://localhost:5432/postgres`\
Type: `required`

PostgresDB URL, it's usually the hostname followed by port.

#### <a name="sink.jdbc.table.name" /> `sink.jdbc.table.name`
Example value: `public.customers`\
Type: `required`

The name of the table in which the data should be dumped.

#### <a name="sink.jdbc.username" /> `sink.jdbc.username`
Example value: `root`\
Type: `required`

The username to connect to DB.

#### <a name="sink.jdbc.password" /> `sink.jdbc.password`
Example value: `root`\
Type: `required`

The password to connect to DB.

#### <a name="input.schema.proto.to.column.mapping" /> `input.schema.proto.to.column.mapping`
Example value: `{"6":"customer_id","1":"service_type","5":"event_timestamp"}` Proto field value with index 1 will be stored in a column named service_type in DB and so on\
Type: `required`

The mapping of fields in DB and the corresponding proto index from where the value will be extracted. This is a JSON field.

#### <a name="sink.jdbc.unique.keys" /> `sink.jdbc.unique.keys`
Example value: `customer_id`\
Type: `optional`

Comma-separated column names having a unique constraint on the table.

#### <a name="sink.jdbc.connection.pool.timeout.ms" /> `sink.jdbc.connection.pool.timeout.ms`
Example value: `1000`\
Type: `required`\
Default value: `1000`

Database connection timeout in milliseconds.

#### <a name="sink.jdbc.connection.pool.idle.timeout.ms" /> `sink.jdbc.connection.pool.idle.timeout.ms`
Example value: `60000`\
Type: `required`\
Default value: `60000`

Database connection pool idle connection timeout in milliseconds.

#### <a name="sink.jdbc.connection.pool.min.idle" /> `sink.jdbc.connection.pool.min.idle`
Example value: `0`\
Type: `required`\
Default value: `0`

The minimum number of idle connections in the pool to maintain.

#### <a name="sink.jdbc.connection.pool.max.size" /> `sink.jdbc.connection.pool.max.size`
Example value: `10`\
Type: `required`\
Default value: `10`

Maximum size for the database connection pool.

### <a name="Influx Sink" /> Influx Sink
An Influx sink firehose requires the following variables to be set along with Generic ones

#### <a name="sink.influx.url" /> `sink.influx.url`
Example value: `http://localhost:8086`\
Type: `required`

InfluxDB URL, it's usually the hostname followed by port.

#### <a name="sink.influx.username" /> `sink.influx.username`
Example value: `root`\
Type: `required`

The username to connect to DB.

#### <a name="sink.influx.password" /> `sink.influx.password`
Example value: `root`\
Type: `required`

The password to connect to DB.

#### <a name="sink.influx.field.name.proto.index.mapping" /> `sink.influx.field.name.proto.index.mapping`
Example value: `"2":"order_number","1":"service_type", "4":"status"`\
Proto field value with index 2 will be stored in a field named 'order_number' in Influx and so on\
Type: `required`

The mapping of fields and the corresponding proto index which can be used to extract the field value from the proto message. This is a JSON field. Note that Influx keeps a single value for each unique set of tags and timestamps. If a new value comes with the same tag and timestamp from the source, it will override the existing one.

#### <a name="sink.influx.tag.name.proto.index.mapping" /> `sink.influx.tag.name.proto.index.mapping`
Example value: `{"6":"customer_id"}`\
Type: `optional`

The mapping of tags and the corresponding proto index from which the value for the tags can be obtained. If the tags contain existing fields from field name mapping it will not be overridden. They will be duplicated. If ENUMS are present then they must be added here. This is a JSON field.

#### <a name="sink.influx.proto.event.timestamp.index" /> `sink.influx.proto.event.timestamp.index`
Example value: `5`\
Type: `required`

Proto index of a field that can be used as the timestamp.

#### <a name="sink.influx.db.name" /> `sink.influx.db.name`
Example value: `status`\
Type: `required`

InfluxDB database name where data will be dumped.

#### <a name="sink.influx.retention.policy" /> `sink.influx.retention.policy`
Example value: `quarterly`\
Type: `required`\
Default value: `autogen`

Retention policy for influx database.

#### <a name="sink.influx.measurement.name" /> `sink.influx.measurement.name`
Example value: `customer-booking`\
Type: `required`

This field is used to give away the name of the measurement that needs to be used by the sink. Measurement is another name for tables and it will be auto-created if does not exist at the time Firehose pushes the data to the influx.

### <a name="Redis Sink" /> Redis Sink
A Redis sink firehose requires the following variables to be set along with Generic ones

#### <a name="sink.redis.urls" /> `sink.redis.urls`
Example value: `localhos:6379,localhost:6380`\
Type: `required`

REDIS instance hostname/IP address followed by its port.

#### <a name="sink.redis.data.type" /> `sink.redis.data.type`
Example value: `Hashset`\
Type: `required`\
Default value: `List`

To select whether you want to push your data as a HashSet or as a List.

#### <a name="sink.redis.key.template" /> `sink.redis.key.template`
Example value: `Service\_%%s,1`\
This will take the value with index 1 from proto and create the Redis keys as per the template\
Type: `required`

The string that will act as the key for each Redis entry. This key can be configured as per the requirement, a constant or can extract value from each message and use that as the Redis key.

#### <a name="input.schema.proto.to.column.mapping" /> `input.schema.proto.to.column.mapping`
Example value: `{"6":"customer_id",  "2":"order_num"}`\
Type: `required (For Hashset)`

This is the field that decides what all data will be stored in the HashSet for each message.

#### <a name="sink.redis.list.data.proto.index" /> `sink.redis.list.data.proto.index`
Example value: `6`\
This will get the value of the field with index 6 in your proto and push that to the Redis list with the corresponding keyTemplate\
Type: `required (For List)`

This field decides what all data will be stored in the List for each message.

#### <a name="sink.redis.ttl.type" /> `sink.redis.ttl.type`
Example value: `DURATION`\
Type: `optional`\
Default value: `DISABLE`

Choice of Redis TTL type.It can be:\
`DURATION`: After which the Key will be expired and removed from Redis (UNIT- seconds)\
`EXACT_TIME`: Precise UNIX timestamp after which the Key will be expired

#### <a name="sink.redis.ttl.value" /> `sink.redis.ttl.value`
Example value: `100000`\
Type: `optional`\
Default value: `0`

Redis TTL value in Unix Timestamp for `EXACT_TIME` TTL type, In Seconds for `DURATION` TTL type.

#### <a name="sink.redis.deployment.type" /> `sink.redis.deployment.type`
Example value: `Standalone`\
Type: `required`\
Default value: `Standalone`

The Redis deployment you are using. At present, we support `Standalone` and `Cluster` types.

### <a name="ElasticSearch Sink" /> ElasticSearch Sink
An ES sink firehose requires the following variables to be set along with Generic ones

#### <a name="sink.es.connection.urls" /> `sink.es.connection.urls`
Example value: `localhost1:9200`\
Type: `required`

Elastic search connection URL/URLs to connect. Multiple URLs could be given in a comma separated format.

#### <a name="sink.es.index.name" /> `sink.es.index.name`
Example value: `sample_index`\
Type: `required`

The name of the index to which you want to write the documents. If it does not exist, it will be created.

#### <a name="sink.es.type.name" /> `sink.es.type.name`
Example value: `Customer`\
Type: `required`

The type name of the Document in ElasticSearch.

#### <a name="sink.es.id.field" /> `sink.es.id.field`
Example value: `customer_id`\
Type: `required`

The identifier field of the document in ElasticSearch. This should be the key of the field present in the message (JSON or Protobuf) and it has to be a unique, non-null field. So the value of this field in the message will be used as the ID of the document in ElasticSearch while writing the document.

#### <a name="sink.es.mode.update.only.enable" /> `sink.es.mode.update.only.enable`
Example value: `TRUE`\
Type: `required`\
Default value: `FALSE`

Elasticsearch sink can be created in 2 modes: Upsert mode or UpdateOnly mode. If this config is set:\
`TRUE`: Firehose will run on UpdateOnly mode which will only UPDATE the already existing documents in the ElasticSearch index.
`FALSE`: Firehose will run on Upsert mode, UPDATING the existing documents and also INSERTING any new ones.

#### <a name="sink.es.input.message.type" /> `sink.es.input.message.type`
Example value: `PROTOBUF`\
Type: `required`\
Default value: `JSON`

Indicates if the Kafka topic contains JSON or Protocol Buffer messages.

#### <a name="sink.es.preserve.proto.field.names.enable" /> `sink.es.preserve.proto.field.names.enable`
Example value: `FALSE`\
Type: `required`\
Default value: `TRUE`

Whether or not the protobuf field names should be preserved in the ElasticSearch document. If false the fields will be converted to camel case.

#### <a name="sink.es.request.timeout.ms" /> `sink.es.request.timeout.ms`
Example value: `60000`\
Type: `required`\
Default value: `60000`

The request timeout of the elastic search endpoint. The value specified is in milliseconds.

#### <a name="sink.es.shards.active.wait.count" /> `sink.es.shards.active.wait.count`
Example value: `1`\
Type: `required`\
Default value: `1`

The number of shard copies that must be active before proceeding with the operation. This can be set to any non-negative value less than or equal to the total number of shard copies (number of replicas + 1).

#### <a name="sink.es.retry.status.code.blacklist" /> `sink.es.retry.status.code.blacklist`
Example value: `404,400`\
Type: `optional`

List of comma-separated status codes for which firehose should not retry in case of UPDATE ONLY mode is TRUE

#### <a name="sink.es.routing.key.name" /> `sink.es.routing.key.name`
Example value: `service_type`\
Type: `optional`

The proto field whose value will be used for routing documents to a particular shard in Elasticsearch. If empty, Elasticsearch uses the ID field of the doc by default.

### <a name="GRPC Sink" /> GRPC Sink
A GRPC sink firehose requires the following variables to be set along with Generic ones

#### <a name="sink.grpc.service.host" /> `sink.grpc.service.host`
Example value: `http://grpc-service.sample.io`\
Type: `required`

The host of the GRPC service.

#### <a name="sink.grpc.service.port" /> `sink.grpc.service.port`
Example value: `8500`\
Type: `required`

Port of the GRPC service.

#### <a name="sink.grpc.method.url" /> `sink.grpc.method.url`
Example value: `com.tests.SampleServer/SomeMethod`\
Type: `required`

URL of the GRPC method that needs to be called.

#### <a name="sink.grpc.response.schema.proto.class" /> `sink.grpc.response.schema.proto.class`
Example value: `com.tests.SampleGrpcResponse`\
Type: `required`

Proto which would be the response of the GRPC Method.

### <a name="Standard" /> Standard

#### <a name="source.kafka.consumer.config.max.poll.records" /> `source.kafka.consumer.config.max.poll.records`
Example value: `500`\
Type: `required`\
Default value: `500`

The maximum number of records, the consumer will fetch from Kafka in one request.

#### <a name="retry.exponential.backoff.initial.ms" /> `retry.exponential.backoff.initial.ms`
Example value: `10`\
Type: `required`\
Default value: `10`

Initial expiry time in milliseconds for exponential backoff policy.

#### <a name="retry.exponential.backoff.rate" /> `retry.exponential.backoff.rate`
Example value: `2`\
Type: `required`\
Default value: `2`

Backoff rate for exponential backoff policy.

#### <a name="retry.exponential.backoff.max.ms" /> `retry.exponential.backoff.max.ms`
Example value: `60000`\
Type: `required`\
Default value: `60000`
Maximum expiry time in milliseconds for exponential backoff policy.

