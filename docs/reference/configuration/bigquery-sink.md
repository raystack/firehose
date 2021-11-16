# Bigquery Sink

A Bigquery sink Firehose \(`SINK_TYPE`=`bigquery`\) requires the following variables to be set along with Generic ones

## `SINK_BIGQUERY_GOOGLE_CLOUD_PROJECT_ID`

Contains information of google cloud project id location of the bigquery table where the records need to be inserted. Further documentation on google cloud [project id](https://cloud.google.com/resource-manager/docs/creating-managing-projects).

* Example value: `gcp-project-id`
* Type: `required`

## `SINK_BIGQUERY_TABLE_NAME`

The name of bigquery table. Here is further documentation of bigquery [table naming](https://cloud.google.com/bigquery/docs/tables).

* Example value: `user_profile`
* Type: `required`

## `SINK_BIGQUERY_DATASET_NAME`

The name of dataset that contains the bigquery table. Here is further documentation of bigquery [dataset naming](https://cloud.google.com/bigquery/docs/datasets).

* Example value: `customer`
* Type: `required`


## `SINK_BIGQUERY_DATASET_LABELS`

Labels of a bigquery dataset, key-value information separated by comma attached to the bigquery dataset. This configuration define labels that will be set to the bigquery dataset. Here is further documentation of bigquery [labels](https://cloud.google.com/bigquery/docs/labels-intro).

* Example value: `owner=data-engineering,granurality=daily`
* Type: `optional`

## `SINK_BIGQUERY_TABLE_LABELS`

Labels of a bigquery table, key-value information separated by comma attached to the bigquery table. This configuration define labels that will be set to the bigquery dataset. Here is further documentation of bigquery [labels](https://cloud.google.com/bigquery/docs/labels-intro).

* Example value: `owner=data-engineering,granurality=daily`
* Type: `optional`

## `SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE`

Configuration for enable table partitioning. This config will be used for provide partitioning config when creating the bigquery table. 
Bigquery table partitioning config can only be set once, on the table creation and the partitioning cannot be disabled once created. Changing this value of this config later will cause error when firehose trying to update the bigquery table. 
Here is further documentation of bigquery [table partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables).

* Example value: `true`
* Type: `required`
* Default value: `false`

## `SINK_BIGQUERY_TABLE_PARTITION_KEY`

Define protobuf/bigquery field name that will be used for bigquery table partitioning. only protobuf `Timestamp` field, that later converted into bigquery `Timestamp` column that is supported as partitioning key.
Currently, this sink only support `DAY` time partitioning type.
Here is further documentation of bigquery [column time partitioning](https://cloud.google.com/bigquery/docs/creating-partitioned-tables#console).

* Example value: `event_timestamp`
* Type: `required`

## `SINK_BIGQUERY_ROW_INSERT_ID_ENABLE`

This config enables adding of ID row intended for deduplication when inserting new records into bigquery.
Here is further documentation of bigquery streaming insert [deduplication](https://cloud.google.com/bigquery/streaming-data-into-bigquery).

* Example value: `false`
* Type: `required`
* Default value: `true`

## `SINK_BIGQUERY_CREDENTIAL_PATH`

Full path of google cloud credentials file. Here is further documentation of google cloud authentication and [credentials](https://cloud.google.com/docs/authentication/getting-started).

* Example value: `/.secret/google-cloud-credentials.json`
* Type: `required`

## `SINK_BIGQUERY_METADATA_NAMESPACE`

The name of column that will be added alongside of the existing bigquery column that generated from protobuf, that column contains struct of kafka metadata of the inserted record.
When this config is not configured the metadata column will not be added to the table.

* Example value: `kafka_metadata`
* Type: `optional`

## `SINK_BIGQUERY_DATASET_LOCATION`

The geographic region name of location of bigquery dataset. Further documentation on bigquery dataset [location](https://cloud.google.com/bigquery/docs/locations#dataset_location).

* Example value: `us-central1`
* Type: `optional`
* Default value: `asia-southeast1`

## `SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS`

The duration of bigquery table partitioning expiration in milliseconds. Fill this config with `-1` will disable the table partition expiration. Further documentation on bigquery table partition [expiration](https://cloud.google.com/bigquery/docs/managing-partitioned-tables#partition-expiration).

* Example value: `2592000000`
* Type: `optional`
* Default value: `-1`

## `SINK_BIGQUERY_CLIENT_READ_TIMEOUT_MS`

The duration of bigquery client http read timeout in milliseconds, 0 for an infinite timeout, a negative number for the default value (20000).

* Example value: `20000`
* Type: `optional`
* Default value: `-1`

## `SINK_BIGQUERY_CLIENT_CONNECT_TIMEOUT_MS`

The duration of bigquery client http connection timeout in milliseconds, 0 for an infinite timeout, a negative number for the default value (20000).

* Example value: `20000`
* Type: `optional`
* Default value: `-1`