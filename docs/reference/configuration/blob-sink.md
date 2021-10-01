# Blob Sink

A Blob sink Firehose \(`SINK_TYPE`=`blob`\) requires the following variables to be set along with Generic ones

## `SINK_BLOB_STORAGE_TYPE`

Defines the types of blob storage the destination remote file system the file will be uploaded. Currently, the only supported blob storage is `GCS` (google cloud storage).

* Example value: `GCS`
* Type: `required`

## `SINK_BLOB_LOCAL_FILE_WRITER_TYPE`

Defines the name of the writer of a file format. Currently, only `PARQUET` file format is supported.

* Example value: `PARQUET`
* Type: `required`

## `SINK_BLOB_WRITE_KAFKA_METADATA_ENABLE`

Define configuration to enable or disable adding kafka metadata to the written records.

* Example value: `true`
* Type: `required`

## `SINK_BLOB_LOCAL_DIRECTORY`

Defines directory temporary files will be created before uploaded to remote destination.

* Example value: `/tmp/firehose/objectstorage`
* Type: `optional`
* Default value: `/tmp/firehose`

## `SINK_BLOB_KAFKA_METADATA_COLUMN_NAME`

Defines the kafka metadata column name. This config determines the schema changes column or field that will be added on the parquet format. When the metadata column name is not configured all metadata column or field will be added on top level. 
When metadata column name is configured, all metadata column/field will be added as child field under the configured column name.

* Example value: `kafka_metadata`
* Type: `optional`

## `SINK_BLOB_WRITER_PARQUET_BLOCK_SIZE`

Defines the storage parquet writer block size, this config only applies on parquet writer. This configuration is only needed to be set manually when user need to control the block size for optimal file read.

* Example value: `134217728`
* Type: `optional`

## `SINK_BLOB_WRITER_PARQUET_PAGE_SIZE`

Define the storage parquet writer page size, this config only applies on parquet writer. This configuration is only needed to be set manually when user need to control the block size for optimal file read.

* Example value: `1048576`
* Type: `optional`

## `SINK_BLOB_FILE_ROTATION_DURATION_MS`

Define the maximum duration of record to be added to a single parquet file in milliseconds, after the elapsed time exceeded the configured duration, current file will be closed, a new file will be created and incoming records will be written to the new file. 

* Example value: `1800000`
* Type: `optional`
* Default value: `3600000`

## `SINK_BLOB_FILE_ROTATION_MAX_SIZE_BYTES`

Defines the maximum size of record to be written on a single parquet file in bytes, new record will be written to new a file.

* Example value: `3600000`
* Type: `required`
* Default value: `268435456`

## `SINK_BLOB_TIME_PARTITIONING_FIELD_NAME`

Defines the field used as file partitioning.

* Example value: `event_timestamp`
* Type: `required`

## `SINK_BLOB_TIME_PARTITIONING_TYPE`

Defines time partitioning file type. Currently, the supported partitioning type are `hour`, `day`. This also affect the partitioning path of the files.

* Example value: `hour`
* Type: `required`
* Default value: `hour`

## `SINK_BLOB_TIME_PARTITIONING_TIME_ZONE`

Defines time partitioning time zone. The date time partitioning uses local date and time value that calculated using the configured timezone.

* Example value: `UTC`
* Type: `optional`
* Default value: `UTC`

## `SINK_BLOB_TIME_PARTITIONING_HOUR_PREFIX`

Defines time partitioning path format for hour segment for example `${date_segment}/hr=10/${filename}`.

* Example value: `hr=`
* Type: `optional`
* Default value: `hr=`

## `SINK_BLOB_TIME_PARTITIONING_DATE_PREFIX`

Defines time partitioning path format for date segment for example `dt=2021-01-01/${hour_segment}/${filename}`.

* Example value: `dt=`
* Type: `optional`
* Default value: `dt=`

## `SINK_BLOB_GCS_GOOGLE_CLOUD_PROJECT_ID`

The identifier of google project ID where the google cloud storage bucket is located. Further documentation on google cloud [project id](https://cloud.google.com/resource-manager/docs/creating-managing-projects).

* Example value: `project-007`
* Type: `required`

## `SINK_BLOB_GCS_BUCKET_NAME`

The name of google cloud storage bucket. Here is further documentation of google cloud storage [bucket name](https://cloud.google.com/storage/docs/naming-buckets). 

* Example value: `pricing`
* Type: `required`

## `SINK_BLOB_GCS_CREDENTIAL_PATH`

Full path of google cloud credentials file. Here is further documentation of google cloud authentication and [credentials](https://cloud.google.com/docs/authentication/getting-started).

* Example value: `/.secret/google-cloud-credentials.json`
* Type: `required`

## `SINK_BLOB_GCS_RETRY_MAX_ATTEMPTS`

Number of retry of the google cloud storage upload request when the request failed.

* Example value: `10`
* Type: `optional`
* Default value: `10`

## `SINK_BLOB_GCS_RETRY_TOTAL_TIMEOUT_MS`

Duration of retry of the google cloud storage upload in milliseconds. Google cloud storage client will keep retry the upload operation until the elapsed time since first retry exceed the configured duration.
Both of the config `SINK_BLOB_GCS_RETRY_MAX_ATTEMPTS` and `SINK_BLOB_GCS_RETRY_TOTAL_TIMEOUT_MS` can works at the same time, exception will be triggered when one of the limit is exceeded, user also need to aware of the default values.  

* Example value: `60000`
* Type: `optional`
* Default value: `120000`

## `SINK_BLOB_GCS_RETRY_INITIAL_DELAY_MS"`

Initial delay for first retry in milliseconds. It is recommended to set this config at default values.

* Example value: `500`
* Type: `optional`
* Default value: `1000`

## `SINK_BLOB_GCS_RETRY_MAX_DELAY_MS"`

Maximum delay for each retry in milliseconds when delay being multiplied because of increase in retry attempts. It is recommended to set this config at default values.

* Example value: `15000`
* Type: `optional`
* Default value: `30000`

## `SINK_BLOB_GCS_RETRY_DELAY_MULTIPLIER"`

Multiplier of retry delay. The new retry delay for the subsequent operation will be recalculated for each retry. This config will cause increase of retry delay. 
When this config is set to `1` means the delay will be constant. It is recommended to set this config at default values.

* Example value: `1.5`
* Type: `optional`
* Default value: `2`

## `SINK_BLOB_GCS_RETRY_INITIAL_RPC_TIMEOUT_MS"`

Initial timeout in milliseconds of RPC call for google cloud storage client. It is recommended to set this config at default values.

* Example value: `3000`
* Type: `optional`
* Default value: `5000`

## `SINK_BLOB_GCS_RETRY_RPC_MAX_TIMEOUT_MS"`

Maximum timeout in milliseconds of RPC call for google cloud storage client. It is recommended to set this config at default values.

* Example value: `10000`
* Type: `optional`
* Default value: `5000`

## `SINK_BLOB_GCS_RETRY_RPC_TIMEOUT_MULTIPLIER"`

Multiplier of google cloud storage client RPC call timeout. When this config is set to `1` means the config is multiplied. It is recommended to set this config at default values.

* Example value: `1`
* Type: `optional`
* Default value: `1`
