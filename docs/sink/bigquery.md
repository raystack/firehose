# Bigquery Sink

Bigquery Sink has several responsibilities, first creation of bigquery table and dataset when they are not exist, second update the bigquery table schema based on the latest protobuf schema, third translate protobuf messages into bigquery records and insert them to bigquery tables. 

## At Least Once Guarantee
Kafka offset commit is managed by firehose, and offset marking will only advance toward the latest kafka offset which message successfully inserted to bigquery, at the first time or after several retries or after sent to DLQ. Firehose consumer ensure messages to be inserted to bigquery table before removed from kafka queue.

## Bigquery Table Schema Update

Bigquery Sink manages the bigquery table schema instead of let the schema changes on table insert through schema relaxation config. Bigquery utilise [Stencil](https://github.com/odpf/stencil) to parse protobuf messages generate schema and update bigquery tables with the latest schema. The stencil client periodically reload the descriptor cache. Table update happened after the descriptor caches uploaded. Because firehose is horizontally scalable when multiple firehose consumer is running, because no coordination strategy between consumers the schema update will be triggered by all consumers.
In the future stencil will automatically detect schema changes and re load the descriptor cache, by examining Unknown Fields when parsing the protobuf message. 

## Protobuf - Bigquery Table Type Mapping

Here are type conversion between protobuf type and bigquery type : 

| Protobuf Type | Bigquery Type |
| --- | ----------- |
| bytes | BYTES |
| string | STRING |
| enum | STRING |
| float | FLOAT |
| double | FLOAT |
| bool | BOOLEAN |
| int64, uint64, int32, uint32, fixed64, fixed32, sfixed64, sfixed32, sint64, sint32 | INTEGER |
| message | RECORD |
| .google.protobuf.Timestamp | TIMESTAMP |
| .google.protobuf.Struct | STRING (Json Serialised) |
| .google.protobuf.Duration | RECORD |

| Protobuf Modifier | Bigquery Modifier |
| --- | ----------- |
| repeated | REPEATED |


## Partitioning

Bigquery Sink supports table partitioning features in bigquery tables. Currently, Bigquery Sink only supports time based partitioning with protobuf `Timestamp` as field and with `DAY` partitioning granurality type.
Table partitioning is applied on table creation.

## Kafka Metadata

When `SINK_BIGQUERY_METADATA_NAMESPACE` is configured kafka metadata column will be added, here is the list of kafka metadata column to be added :

| Fully Qualified Column Name | Type | Modifier |
| --- | ----------- | ------- | 
| metadata_column | RECORD | NULLABLE |
| metadata_column.message_partition | INTEGER | NULLABLE |
| metadata_column.message_offset | INTEGER | NULLABLE |
| metadata_column.message_topic | STRING | NULLABLE |
| metadata_column.message_timestamp | TIMESTAMP | NULLABLE |
| metadata_column.load_time | TIMESTAMP | NULLABLE |

## Errors Handling

Firehose consumer parse errors from table insertion, translate the error into generic error types and attach them for each message that failed to be inserted to bigquery. Users can configure how to handle each generic error types accordingly. Here is mapping of the error translation to generic firehose error types : 

* Stopped error when insert job is cancelled because other record is invalid, is translated to `SINK_5XX_ERROR`
* Out of bounds are caused when the partitioned column has a date value less than 5 years and more than 1 year in the future, is translated to `SINK_4XX_ERROR`
* Invalid schema errors when there is new field that is not exist on the table or when there is required field on the table, is translated to `SINK_4XX_ERROR`
* Other error is translated to `SINK_UNKNOWN_ERROR`