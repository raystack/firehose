# BigQuery

Bigquery Sink has several responsibilities, first creation of bigquery table and dataset when they are not exist, second update the bigquery table schema based on the latest protobuf schema, third translate protobuf messages into bigquery records and insert them to bigquery tables.
Bigquery utilise Bigquery [Streaming API](https://cloud.google.com/bigquery/streaming-data-into-bigquery) to insert record into bigquery tables.

## Asynchronous consumer mode

Bigquery Streaming API limits size of payload sent for each insert operations. The limitation reduces the amount of message allowed to be inserted when the message size is big.
This will reduce the throughput of bigquery sink. To increase the throughput, firehose provide kafka consumer asynchronous mode.
In asynchronous mode sink operation is executed asynchronously, so multiple sink task can be scheduled and run concurrently.
Throughput can be increased by increasing the number of sink pool.

## At Least Once Guarantee

Because of asynchronous consumer mode and the possibility of retry on the insert operation. There is no guarantee of the message order that successfully sent to the sink.
That also happened with commit offset, the there is no order of the offset number of the processed messages.
Firehose collect all the offset sort them and only commit the latest continuous offset.
This will ensure all the offset being committed after messages successfully processed even when some messages are being re processed by retry handler or when the insert operation took a long time.

## Bigquery table schema update

Bigquery Sink update the bigquery table schema on separate table update operation. Bigquery utilise [Stencil](https://github.com/odpf/stencil) to parse protobuf messages generate schema and update bigquery tables with the latest schema.
The stencil client periodically reload the descriptor cache. Table schema update happened after the descriptor caches uploaded.
Because firehose is horizontally scalable multiple firehose consumer might be running.
Because there is no coordination strategy between consumers the schema update will be triggered by all consumers.

## Protobuf and BigQuery table type mapping

Here are type conversion between protobuf type and bigquery type :

| Protobuf Type                                                                      | Bigquery Type            |
| ---------------------------------------------------------------------------------- | ------------------------ |
| bytes                                                                              | BYTES                    |
| string                                                                             | STRING                   |
| enum                                                                               | STRING                   |
| float                                                                              | FLOAT                    |
| double                                                                             | FLOAT                    |
| bool                                                                               | BOOLEAN                  |
| int64, uint64, int32, uint32, fixed64, fixed32, sfixed64, sfixed32, sint64, sint32 | INTEGER                  |
| message                                                                            | RECORD                   |
| .google.protobuf.Timestamp                                                         | TIMESTAMP                |
| .google.protobuf.Struct                                                            | STRING (Json Serialised) |
| .google.protobuf.Duration                                                          | RECORD                   |

## Modifier

| Protobuf Modifier | Bigquery Modifier |
| ----------------- | ----------------- |
| repeated          | REPEATED          |

## Partitioning

Bigquery Sink supports creation of table with partition configuration. Currently, Bigquery Sink only supports time based partitioning.
To have time based partitioning protobuf `Timestamp` as field is needed on the protobuf message. The protobuf field will be used as partitioning column on table creation.
The time partitioning type that is currently supported is `DAY` partitioning.

## Kafka Metadata

For data quality checking purpose sometimes kafka metadata need to be added on the record. When `SINK_BIGQUERY_METADATA_NAMESPACE` is configured kafka metadata column will be added, here is the list of kafka metadata column to be added :

| Fully Qualified Column Name       | Type      | Modifier |
| --------------------------------- | --------- | -------- |
| metadata_column                   | RECORD    | NULLABLE |
| metadata_column.message_partition | INTEGER   | NULLABLE |
| metadata_column.message_offset    | INTEGER   | NULLABLE |
| metadata_column.message_topic     | STRING    | NULLABLE |
| metadata_column.message_timestamp | TIMESTAMP | NULLABLE |
| metadata_column.load_time         | TIMESTAMP | NULLABLE |

## Error handling

Firehose consumer parse errors from table insertion, translate the error into generic error types and attach them for each message that failed to be inserted to bigquery.
Users can configure how to handle each generic error types accordingly.
Here is mapping of the error translation to generic firehose error types :

| Error Name           | Generic Error Type | Description                                                                                                                             |
| -------------------- | ------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| Stopped Error        | SINK_5XX_ERROR     | Error on a row insertion that happened because insert job is cancelled because other record is invalid although current record is valid |
| Out of bounds Error  | SINK_4XX_ERROR     | Error on a row insertion the partitioned column has a date value less than 5 years and more than 1 year in the future                   |
| Invalid schema Error | SINK_4XX_ERROR     | Error on a row insertion when there is a new field that is not exist on the table or when there is required field on the table          |
| Other Error          | SINK_UNKNOWN_ERROR | Uncategorized error                                                                                                                     |

## Google Cloud Bigquery IAM Permission

Several IAM permission is required for bigquery sink to run properly,

- Create and update Dataset
  - bigquery.tables.create
  - bigquery.tables.get
  - bigquery.tables.update
- Create and update Table
  - bigquery.datasets.create
  - bigquery.datasets.get
  - bigquery.datasets.update
- Stream insert to Table
  - bigquery.tables.updateData

Further documentation on bigquery IAM permission [here](https://cloud.google.com/bigquery/streaming-data-into-bigquery).

## Configurations

A Bigquery sink Firehose \(`SINK_TYPE`=`bigquery`\) requires the following variables to be set along with Generic ones

### `SINK_BIGQUERY_GOOGLE_CLOUD_PROJECT_ID`

Contains information of google cloud project id location of the bigquery table where the records need to be inserted. Further documentation on google cloud [project id](https://cloud.google.com/resource-manager/docs/creating-managing-projects).

- Example value: `gcp-project-id`
- Type: `required`

### `SINK_BIGQUERY_TABLE_NAME`

The name of bigquery table. Here is further documentation of bigquery [table naming](https://cloud.google.com/bigquery/docs/tables).

- Example value: `user_profile`
- Type: `required`

### `SINK_BIGQUERY_DATASET_NAME`

The name of dataset that contains the bigquery table. Here is further documentation of bigquery [dataset naming](https://cloud.google.com/bigquery/docs/datasets).

- Example value: `customer`
- Type: `required`

### `SINK_BIGQUERY_DATASET_LABELS`

Labels of a bigquery dataset, key-value information separated by comma attached to the bigquery dataset. This configuration define labels that will be set to the bigquery dataset. Here is further documentation of bigquery [labels](https://cloud.google.com/bigquery/docs/labels-intro).

- Example value: `owner=data-engineering,granurality=daily`
- Type: `optional`

### `SINK_BIGQUERY_TABLE_LABELS`

Labels of a bigquery table, key-value information separated by comma attached to the bigquery table. This configuration define labels that will be set to the bigquery dataset. Here is further documentation of bigquery [labels](https://cloud.google.com/bigquery/docs/labels-intro).

- Example value: `owner=data-engineering,granurality=daily`
- Type: `optional`

### `SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE`

Configuration for enable table partitioning. This config will be used for provide partitioning config when creating the bigquery table.
Bigquery table partitioning config can only be set once, on the table creation and the partitioning cannot be disabled once created. Changing this value of this config later will cause error when firehose trying to update the bigquery table.
Here is further documentation of bigquery [table partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables).

- Example value: `true`
- Type: `required`
- Default value: `false`

### `SINK_BIGQUERY_TABLE_PARTITION_KEY`

Define protobuf/bigquery field name that will be used for bigquery table partitioning. only protobuf `Timestamp` field, that later converted into bigquery `Timestamp` column that is supported as partitioning key.
Currently, this sink only support `DAY` time partitioning type.
Here is further documentation of bigquery [column time partitioning](https://cloud.google.com/bigquery/docs/creating-partitioned-tables#console).

- Example value: `event_timestamp`
- Type: `required`

### `SINK_BIGQUERY_ROW_INSERT_ID_ENABLE`

This config enables adding of ID row intended for deduplication when inserting new records into bigquery.
Here is further documentation of bigquery streaming insert [deduplication](https://cloud.google.com/bigquery/streaming-data-into-bigquery).

- Example value: `false`
- Type: `required`
- Default value: `true`

### `SINK_BIGQUERY_CREDENTIAL_PATH`

Full path of google cloud credentials file. Here is further documentation of google cloud authentication and [credentials](https://cloud.google.com/docs/authentication/getting-started).

- Example value: `/.secret/google-cloud-credentials.json`
- Type: `required`

### `SINK_BIGQUERY_METADATA_NAMESPACE`

The name of column that will be added alongside of the existing bigquery column that generated from protobuf, that column contains struct of kafka metadata of the inserted record.
When this config is not configured the metadata column will not be added to the table.

- Example value: `kafka_metadata`
- Type: `optional`

### `SINK_BIGQUERY_DATASET_LOCATION`

The geographic region name of location of bigquery dataset. Further documentation on bigquery dataset [location](https://cloud.google.com/bigquery/docs/locations#dataset_location).

- Example value: `us-central1`
- Type: `optional`
- Default value: `asia-southeast1`

### `SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS`

The duration of bigquery table partitioning expiration in milliseconds. Fill this config with `-1` will disable the table partition expiration. Further documentation on bigquery table partition [expiration](https://cloud.google.com/bigquery/docs/managing-partitioned-tables#partition-expiration).

- Example value: `2592000000`
- Type: `optional`
- Default value: `-1`

### `SINK_BIGQUERY_CLIENT_READ_TIMEOUT_MS`

The duration of bigquery client http read timeout in milliseconds, 0 for an infinite timeout, a negative number for the default value (20000).

- Example value: `20000`
- Type: `optional`
- Default value: `-1`

### `SINK_BIGQUERY_CLIENT_CONNECT_TIMEOUT_MS`

The duration of bigquery client http connection timeout in milliseconds, 0 for an infinite timeout, a negative number for the default value (20000).

- Example value: `20000`
- Type: `optional`
- Default value: `-1`
