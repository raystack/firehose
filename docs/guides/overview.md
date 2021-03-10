
#### Create a Log Sink
**A log sink firehose requires the following [variables](../reference/configuration.md#a-namegeneric--generic) to be set.**

#### Create an HTTP Sink
**Data read from Kafka is written to an HTTP endpoint and it requires the following [variables](../reference/configuration.md#a-namehttp-sink--http-sink) to be set. You need to create your own HTTP endpoint so that the Firehose can send data to it.**

**Usage of [`sink.http.json.body.template`](../reference/configuration.md#a-namesinkhttpjsonbodytemplate--sinkhttpjsonbodytemplate) is explained here.**

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
* Supports only PostgresDB as of now.
* Data read from Kafka is written to the PostgresDB database and it requires the following [variables](../reference/configuration.md#a-namejdbc-sink--jdbc-sink) to be set.

***Note: Schema (Table, Columns, and Any Constraints) being used in firehose configuration must exist in the Database already.***

#### Create an InfluxDB Sink
* Data read from Kafka is written to the InfluxDB time-series database and it requires the following [variables](../reference/configuration.md#a-nameinflux-sink--influx-sink) to be set.

***Note: [DATABASE](../reference/configuration.md#a-namesinkinfluxdbname--sinkinfluxdbname) and [RETENTION POLICY](../reference/configuration.md#a-namesinkinfluxretentionpolicy--sinkinfluxretentionpolicy) being used in firehose configuration must exist already in the Influx, It’s outside the scope of a firehose and won’t be generated automatically.***

#### Create a Redis Sink

* Redis sink can be created in 2 different modes based on the value of [`sink.redis.data.type`](../reference/configuration.md#a-namesinkredisdatatype--sinkredisdatatype): Hashset or List
    * Hashset: For each message, an entry of the format ‘key : field : value’ is generated and pushed to Redis. field and value are generated on the basis of the config [`input.schema.proto.to.column.mapping`](https://github.com/odpf/firehose/blob/documentation/docs/reference/configuration.md#-inputschemaprototocolumnmapping-2)
    * List: For each message entry of the format ‘key : value’ is generated and pushed to Redis. Value is fetched for the proto index provided in the config [`sink.redis.list.data.proto.index`](../reference/configuration.md#a-namesinkredislistdataprotoindex--sinkredislistdataprotoindex)
* The ‘key’ is picked up from a field in the message itself.
* Limitation: Firehose Redis sink only supports HashSet and List entries as of now.
* it requires the following variables to be set.

#### Create an ElasticSearch Sink

* In the ElasticSearch sink, each message is converted into a document in the specified index with the Document type and ID as specified by the user.
* ElasticSearch sink supports reading messages in both JSON and Protobuf formats.
* it requires the following [variables](../reference/configuration.md#a-nameelasticsearch-sink--elasticsearch-sink) to be set.

#### Create a GRPC Sink

* Data read from Kafka is written to a GRPC endpoint and it requires the following [variables](../reference/configuration.md#a-namegrpc-sink--grpc-sink) to be set.
* You need to create your own GRPC endpoint so that the Firehose can send data to it. The response proto should have a field “success” with value as true or false.

#### Define Standard Configurations

* These are the configurations that remain common across all the Sink Types.
* You don’t need to modify them necessarily, It is recommended to use them with the default values. More details [here](../reference/configuration.md#a-namestandard--standard).

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
