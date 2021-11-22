# Frequently Asked Questions

This section is an attempt to gather some commonly asked queries about Firehose and related components and provide some
answers.

# Table of contents

- [Frequently Asked Questions](#frequently-asked-questions)
    - [Firehose Sinks](#firehose-sinks)
        - [Blob Sink](#blob-sink)
            - [What file formats are the records written into ?](#what-file-formats-are-the-records-written-into-)
            - [Does the sink support json/Avro/Parquet/CSV ?](#does-the-sink-support-jsonavroparquetcsv-)
            - [Does the sink support file compression GZIP/Snappy/lz4 ?](#does-the-sink-support-file-compression-gzipsnappylz4-)
            - [Does the sink support partitioning ?](#does-the-sink-support-partitioning-)
            - [How to configure partitioning ?](#how-to-configure-partitioning-)
            - [Does the sink support S3 or hdfs ?](#does-the-sink-support-s3-or-hdfs-)
            - [How the schema being generated ? How the data types being converted to Parquet types ?](#how-the-schema-being-generated--how-the-data-types-being-converted-to-parquet-types-)
            - [How is the folder path for the file that being created ?](#how-is-the-folder-path-for-the-file-that-being-created-)
            - [How to configure object storage bucket ?](#how-to-configure-object-storage-bucket-)
            - [What will happen when file upload failed ?](#what-will-happen-when-file-upload-failed-)
            - [What will happen when disk is full ?](#what-will-happen-when-disk-is-full-)
            - [How to implement new Object Storage provider, for example S3 ?](#how-to-implement-new-object-storage-provider-for-example-s3-)
            - [How to implement new file format ?](#how-to-implement-new-file-format-)
            - [How to commit being handled ?](#how-to-commit-being-handled-)
            - [How much Disk size recommended for the sink ?](#how-much-disk-size-recommended-for-the-sink-)
        - [BigQuery Sink](#bigquery-sink)
            - [What is the method that is being used to insert the bq rows ?](#what-is-the-method-that-is-being-used-to-insert-the-bq-rows-)
            - [Does the sink support partitioned table ?](#does-the-sink-support-partitioned-table-)
            - [How to configure partitioning ?](#how-to-configure-partitioning-)
            - [How the schema being generated ? How the data types being converted to BQ ?](#how-the-schema-being-generated--how-the-data-types-being-converted-to-bq-)
            - [Does the sink support ingestion time/ integer range partitioning?](#does-the-sink-support-ingestion-time-integer-range-partitioning)
            - [How to configure table destination ?](#how-to-configure-table-destination-)
            - [What will happen when on insertion of a record the timestamp is out of range more than 5 year in the past or 1 year in the future ?](#what-will-happen-when-on-insertion-of-a-record-the-timestamp-is-out-of-range-more-than-5-year-in-the-past-or-1-year-in-the-future-)
            - [How many records are inserted/ batched each time ?](#how-many-records-are-inserted-batched-each-time-)
            - [When is the BigQuery table schema updated ?](#when-is-the-bigquery-table-schema-updated-)
            - [Is the table automatically created ?](#is-the-table-automatically-created-)
            - [Does this sink support BigQuery table clustering configuration ?](#does-this-sink-support-bigquery-table-clustering-configuration-)
            - [Does this sink support BigQuery table labeling ?](#does-this-sink-support-bigquery-table-labeling-)
        - [Redis Sink](#redis-sink)
            - [What is the Redis version supported ?](#what-is-the-redis-version-supported-)
            - [What Data types are supported in Redis sink?](#what-data-types-are-supported-in-redis-sink)
            - [How to use Redis cluster for Redis sink?](#how-to-use-redis-cluster-for-redis-sink)
            - [What all deployments types of Redis is supported ?](#what-all-deployments-types-of-redis-is-supported-)
            - [How to specify a template for the keys ?](#how-to-specify-a-template-for-the-keys-)
            - [How to select nested fields?](#how-to-select-nested-fields)
            - [What is the behaviour on connection failures?](#what-is-the-behaviour-on-connection-failures)
            - [How can TTL be configured for the Redis keys?](#how-can-ttl-be-configured-for-the-redis-keys)
            - [Does it support deleting the keys?](#does-it-support-deleting-the-keys)
            - [What are some of the use cases of this sink?](#what-are-some-of-the-use-cases-of-this-sink)
            - [What happens if the Redis goes down?](#what-happens-if-the-redis-goes-down)

## Firehose Sinks

### Blob Sink

#### What file formats are the records written into ?

Files are written as Parquet file format. For more details, please
look [here](https://parquet.apache.org/documentation/latest/)

#### Does the sink support json/Avro/Parquet/CSV ?

Though the sink only supports writing data as parquet files as of the latest release, other file formats can be added as
well.

#### Does the sink support file compression GZIP/Snappy/lz4 ?

Firehose Blob Sink uses GZIP codec for data compression during the generation of Parquet files.

#### Does the sink support partitioning ?

Yes, Firehose Blob Sink supports hourly and date wise partitioning.

#### How to configure partitioning ?

Partitioning can be configured by setting the config **SINK_BLOB_FILE_PARTITION_TIME_GRANULARITY_TYPE** to HOUR for
hourly partitions or to DATE for date based partitioning. The timestamp field of the message which needs to be used for
partitioning can be configured via **SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_FIELD_NAME** config.

#### Does the sink support S3 or hdfs ?

Though the sink only supports uploading files to Google Cloud Storage(GCS) as of the latest release, other storage
providers can be added as well.

#### How the schema being generated ? How the data types being converted to Parquet types ?

Schema of the Kafka message is read from the proto class specified via configs. Stencil is used to serialize the message
byte array into a Protobuf Dynamic Message using this proto. It is then finally written to a parquet file. In case a
stencil registry is configured, the schema changes will be dynamically updated.

#### How is the folder path for the file that being created ?

The filename is generated as a random UUID string. For the local file, base directory path is taken from the config  
**SINK_BLOB_LOCAL_DIRECTORY** and by default it is set to `/tmp/firehose`. Within this directory, subdirectories  
are generated with names based on whether partitioning is enabled or not:

1. If **SINK_BLOB_FILE_PARTITION_TIME_GRANULARITY_TYPE** is set to NONE meaning partitioning is disabled, the parquet
   files created are placed in directories with name set to the topic name of the kafka messages being processed.
2. If **SINK_BLOB_FILE_PARTITION_TIME_GRANULARITY_TYPE** is set to DAY, the parquet files generated are placed in
   directories  
   with names in the format [topic-name]/[date-prefix][date], where,  
   i. [topic-name] is the topic name of the Kafka message  
   ii. [date-prefix] is a config fetched from **SINK_BLOB_FILE_PARTITION_TIME_DATE_PREFIX** and is set to `dt=` as
   default,  
   iii. [date] is the date in `yyyy-MM-dd` format extracted from a timestamp column of the Kafka message as specified
   via  
   **SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_FIELD_NAME** and converted to the timezone of  
   **SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_TIMEZONE** (default UTC)
3. If **SINK_OBJECT_STORAGE_TIME_PARTITIONING_TYPE** is set to HOUR, the parquet files generated are placed in
   directories  
   with names in the format [topic-name]/[date-prefix][date]/[hour-prefix][hour], where,  
   i. [hour-prefix] is a config fetched from **SINK_BLOB_FILE_PARTITION_TIME_HOUR_PREFIX** and is set to `hr=` as
   default,  
   ii. [hour] is the hour in `HH` format extracted from a timestamp column of the Kafka message as specified via  
   **SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_FIELD_NAME** and converted to the timezone of  
   **SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_TIMEZONE**(default UTC).

#### How to configure object storage bucket ?

Firehose will automatically configure the GCS bucket upon starting by reading the credentials, project id, bucket name
and other settings as configured via environment variables. Please look at the Blob Sink configs for more details.

#### What will happen when file upload failed ?

In case the file upload fails, the thread processing the file upload throws an exception which ultimately bubbles up and
causes the Firehose JVM to get terminated with an abnormal status code.

#### What will happen when disk is full ?

In case the file upload fails, an IOException is thrown which ultimately bubbles up and causes the Firehose JVM to get
terminated with an abnormal status code.

#### How to implement new Object Storage provider, for example S3 ?

Firehose exposes an interface class called *BlobStorage.java* which can be used to create new blob storage
implementations. Subsequently, the *BlobStorageFactory.java* can be modified to produce instance of this new blob
storage provider, while injecting the necessary configs. Firehose also exposes a config called as **
SINK_BLOB_STORAGE_TYPE** which is used to control which storage provider to use, with its default currently set to GCS.

#### How to implement new file format ?

Firehose exposes an interface called *LocalFileWriter.java* which can be used to create new file writer implementations.
Subsequently, the *LocalStorage.java* class can be modified to produce instance of this new file writer, while injecting
the necessary configs. Firehose also exposes a config called as **SINK_BLOB_LOCAL_FILE_WRITER_TYPE** which is used to
control which file writer type to use, with its default currently set to parquet.

#### How to commit being handled ?

ObjectStorage sink does its own offset management. After the polled kafka messages are processed by the sink and the
parquet files are uploaded to object storage and cleaned from local disk as per the policy, committable offsets are then
recorded by the sink. The offsets are then committed as per the Offset Manager. Please look at the Offset Manager class
for more details.

#### How much Disk size recommended for the sink ?

A safe disk size will depend on and can be tuned based on configs set such as the time duration since creation after
which files are pushed to the Blob Storage (**SINK_BLOB_LOCAL_FILE_ROTATION_DURATION_MS**), max parquet file size after
which it is pushed to the Blob Storage(**SINK_BLOB_LOCAL_FILE_ROTATION_MAX_SIZE_BYTES**) as well as the number of topics
that Firehose is consuming from.

### BigQuery Sink

#### What is the method that is being used to insert the bq rows ?

Rows are inserted using streaming insert.

#### Does the sink support partitioned table ?

Yes, BigQuery sink supports writing to partitioned tables as well.

#### How to configure partitioning ?

Partitioning can be enabled by setting the config **SINK_BIGQUERY_ENABLE_TABLE_PARTITIONING_ENABLE** to true. A
partitioning key can be specified via the config **SINK_BIGQUERY_TABLE_PARTITION_KEY**. The time for expiry for date in
a partition can also be configured using **SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS**.

#### How the schema being generated ? How the data types being converted to BQ ?

The schema of the BQ table is generated as per the schema of the input Kafka message proto and is specified via **
INPUT_SCHEMA_PROTO_CLASS**. The Protobuf types are mapped to Legacy SQL types ( BigQuery types). Nested fields and lists
are mapped as type RECORD and REPEATED fields respectively. If Stencil is configured to dynamically fetch the updated
proto, the schema of the BQ table is modified accordingly as and when the input message's proto changes in the Stencil
cache. As per constraints set by BigQuery, a maximum of 15 levels of nesting can be de-serialised and mapped into
corresponding BQ data types beyond which the field values are simply serialised into strings. During the serialisation,
the top level field names in the input Kafka message are mapped to column names in the Big Query table.

#### Does the sink support ingestion time/ integer range partitioning?

No. As of now, Firehose BQ Sink only supports TimePartitioning. The only supported partition keys can be of type DATE or
TIMESTAMP.

#### How to configure table destination ?

The geographic location of the dataset can be set using **SINK_BIGQUERY_DATASET_LOCATION**. Please note that this is a
one-time only change and becomes immutable once set. Changing the config later will cause an exception to be thrown. The
name of the table can be configured by specifying the config **SINK_BIGQUERY_TABLE_NAME**.

#### What will happen when on insertion of a record the timestamp is out of range more than 5 year in the past or 1 year in the future ?

Firehose uses the
legacy [BQ Streaming API](https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll) and only allows
setting a partition key of type DATE or TIMESTAMP column. When Firehose streams data to the table, BigQuery
automatically puts the data into the correct partition, based on the values in the partition column. Records with
partition column value that is between 5 years in the past and 1 year in the future can be inserted, however data
outside this range is rejected. If **SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS** is set, then BigQuery will delete the
data in the partition once this much time has elapsed since the creation of the partition.

#### How many records are inserted/ batched each time ?

All the records fetched in one poll from the Kafka broker are pushed in one batch to BigQuery. This can be controlled by
the config **SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS**.

#### When is the BigQuery table schema updated ?

If Stencil is configured to dynamically fetch the updated proto, the schema of the BigQuery table is modified
accordingly as and when the input message's proto changes in the Stencil cache.

#### Is the table automatically created ?

Yes, Firehose will follow an upsert based model whereby if the BQ table or the dataset doesn't exist, then it will
create them based on the set configs prior to pushing the records.

#### Does this sink support BigQuery table clustering configuration ?

No, table clustering is not yet supported as of the latest release.

#### Does this sink support BigQuery table labeling ?

Yes, both table labels and dataset labels can be specified by setting them in configs **SINK_BIGQUERY_TABLE_LABELS**
and **SINK_BIGQUERY_DATASET_LABELS** respectively. For example, **SINK_BIGQUERY_TABLE_LABELS** can be set
to `label1=value1,label2=value2`.

### Redis Sink

#### What is the Redis version supported ?

Firehose uses Jedis v3.0.1 as of the latest release and hence any Redis version compatible with it can be used.

#### What Data types are supported in Redis sink?

Redis Sink supports persisting the input Kafka messages as LIST and HASHSET data types into Redis.

#### How to use Redis cluster for Redis sink?

To use Redis cluster as the sink, set the following configs as follows:

1. **SINK_REDIS_URLS** to be set to the cluster node URLs separated by comma as a delimiter. For
   example, `127.0.0.1:30004,127.0.0.1:30002,127.0.0.1:30003`
2. **SINK_REDIS_DEPLOYMENT_TYPE** to be set as CLUSTER

#### What all deployments types of Redis is supported ?

Redis Sink currently supports Standalone and Cluster deployments of Redis.

#### How to specify a template for the keys ?

The template key can be specified using the config **SINK_REDIS_KEY_TEMPLATE**. This can be set to either :

1. A constant
2. As a template string: for example, `Service\_%%s,1`. Here, Firehose will extract the value of the incoming message
   field having protobuf index 1 and create the Redis key as per the template.

#### How to select nested fields?

Nested fields can only be selected from the incoming Kafka message when the data type to be pushed to Redis is set as
HashSet. To do so, ensure **SINK_REDIS_DATA_TYPE** config is set to HASHSET and **INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING**
config is set as a JSON indicating the mapping of input message proto field numbers to the desired field names in the
redis hash set entry. For example, consider a proto as follows:-

    message Driver {
        int32 driver_id = 1;
        PersonName driver_name = 2;
    }
    
    message PersonName {
        string fname = 1;
        string lname = 2;
    }

Now consider that each entry in the Redis is required to be a HASHSET consisting of driver id as the key and the
driver's first name and last name as the value fields with names as `first_name` and `last_name` respectively.
Accordingly, the **INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING** config can be set as follows:-

    {""2"":""{\""1\"":\""first_name\"", \""2\"":\""last_name\""}""}"

#### What is the behaviour on connection failures?

If messages failed to get pushed to Redis because of some error( either due to connection timeout or Redis server
failure), Firehose will retry sending the messages for a fixed number of attempts depending on whether a retryable
exception was thrown. If the messages still failed to get published after retries, Firehose will push these messages to
the DLQ topic if DLQ is enabled via configs. If not enabled, it will drop the messages.

#### How can TTL be configured for the Redis keys?

The TTL can be set in 2 ways :-

1. Expiring keys after some duration: Set **SINK_REDIS_TTL_TYPE** to `DURATION` and **SINK_REDIS_TTL_VALUE** to the
   duration in seconds.
2. Expiring keys at some fixed time: Set **SINK_REDIS_TTL_TYPE** to `EXACT_TIME` and **SINK_REDIS_TTL_VALUE** to
   the `UNIX` timestamp when the key needs to expire.

#### Does it support deleting the keys?

Keys can be deleted by configuring a TTL. To know how to set the TTL, please
look [here](#how-can-ttl-be-configured-for-the-redis-keys).

#### What are some of the use cases of this sink?

Some possible use cases could be:-

1. High throughput data generated by one application needs to be streamed back to another application for fast lookup
   purposes. For example, recommendations based on customer interactions.
2. Constant schema but fast changing value of an external entity as published by one application needs to be quickly
   communicated to multiple services to maintain an accurate representation of the entity in internal systems. For
   example, caching of GCM keys in push notification systems.

#### What happens if the Redis goes down?

If messages failed to get pushed to Redis because of some error( either due to connection timeout or Redis server
failure), Firehose will retry sending the messages for a fixed number of attempts depending on whether a retryable
exception was thrown. If the messages still failed to get published after retries, Firehose will push these messages to
the DLQ topic if DLQ is enabled via configs. If not enabled, it will drop the messages.