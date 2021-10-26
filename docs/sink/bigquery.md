# Bigquery Sink

Bigquery Sink has several responsibilities, first creation of bigquery table and dataset when they are not exist, second update the bigquery table schema based on the latest protobuf schema, third translate protobuf messages into bigquery records and insert them to bigquery tables.
Bigquery utilise Bigquery [Streaming API](https://cloud.google.com/bigquery/streaming-data-into-bigquery) to insert record into bigquery tables.

## Asynchronous Consumer

Bigquery Streaming API limits size of payload sent for each insert operations. The limitation reduces the amount of message allowed to be inserted when the message size is big.
This will reduce the throughput of bigquery sink. To increase the throughput. Firehose provide asynchronous consumer mode. In asynchronous consumer mode, firehose can have more than one internal consumer that send message to sink concurrently.
Throughput can be increased by increasing the number of consumer.

## At Least Once Guarantee

Because of asynchronous consumer mode can be configured for bigquery sink and possibility of retry on the insert operation. There is no guarantee of ordering of the message that successfully sent to the sink. 
Similar with commit offset, the there is no order of the offset number of the processed messages. 
Firehose collect all the offset sort them and only commit the latest continuous offset. 
This will ensure all the offset being committed after messages successfully processed even when some messages are being re processed by retry handler or when the insert operation took a long time.

## Bigquery Table Schema Update

Bigquery Sink update the bigquery table schema on separate table update operation. Bigquery utilise [Stencil](https://github.com/odpf/stencil) to parse protobuf messages generate schema and update bigquery tables with the latest schema. 
The stencil client periodically reload the descriptor cache. Table schema update happened after the descriptor caches uploaded. 
Because firehose is horizontally scalable multiple firehose consumer might be running.
Because there is no coordination strategy between consumers the schema update will be triggered by all consumers. 

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

Bigquery Sink supports creation of table with partition configuration. Currently, Bigquery Sink only supports time based partitioning.
To have time based partitioning protobuf `Timestamp` as field is needed on the protobuf message. The protobuf field will be used as partitioning column on table creation. 
The time partitioning type that is currently supported is `DAY` partitioning.

## Kafka Metadata

For data quality checking purpose sometimes kafka metadata need to be added on the record. When `SINK_BIGQUERY_METADATA_NAMESPACE` is configured kafka metadata column will be added, here is the list of kafka metadata column to be added :

| Fully Qualified Column Name | Type | Modifier |
| --- | ----------- | ------- | 
| metadata_column | RECORD | NULLABLE |
| metadata_column.message_partition | INTEGER | NULLABLE |
| metadata_column.message_offset | INTEGER | NULLABLE |
| metadata_column.message_topic | STRING | NULLABLE |
| metadata_column.message_timestamp | TIMESTAMP | NULLABLE |
| metadata_column.load_time | TIMESTAMP | NULLABLE |

## Errors Handling

Firehose consumer parse errors from table insertion, translate the error into generic error types and attach them for each message that failed to be inserted to bigquery. 
Users can configure how to handle each generic error types accordingly. 
Here is mapping of the error translation to generic firehose error types : 

| Error Name | Generic Error Type | Description |
| --- | ----------- | ------- | 
| Stopped Error | SINK_5XX_ERROR | Error on a row insertion that happened because insert job is cancelled because other record is invalid although current record is valid |
| Out of bounds Error | SINK_4XX_ERROR | Error on a row insertion the partitioned column has a date value less than 5 years and more than 1 year in the future |
| Invalid schema Error | SINK_4XX_ERROR | Error on a row insertion when there is a new field that is not exist on the table or when there is required field on the table |
| Other Error | SINK_UNKNOWN_ERROR | Uncategorized error |

## Google Cloud Bigquery IAM Permission

Several IAM permission is required for bigquery sink to run properly,

* Create and update Dataset
    * bigquery.tables.create
    * bigquery.tables.get
    * bigquery.tables.update
* Create and update Table
    * bigquery.datasets.create
    * bigquery.datasets.get
    * bigquery.datasets.update
* Stream insert to Table
    * bigquery.tables.updateData

Further documentation on bigquery IAM permission [here](https://cloud.google.com/bigquery/streaming-data-into-bigquery).