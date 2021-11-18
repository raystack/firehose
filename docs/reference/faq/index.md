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

## Firehose Sinks

### Blob Sink

#### What file formats are the records written into ?
Files are written as Parquet file format. For more details, please look [here](https://parquet.apache.org/documentation/latest/)

#### Does the sink support json/Avro/Parquet/CSV ?
Though the sink only supports writing data as parquet files as of the latest release, other file formats can be added as well.

#### Does the sink support file compression GZIP/Snappy/lz4 ?
Firehose GCS Sink uses GZIP codec for data compression during the generation of Parquet files.

#### Does the sink support partitioning ?
Yes, Firehose GCS Sink supports hourly and date wise partitioning.

#### How to configure partitioning ?
Partitioning can be configured by setting the config **SINK_BLOB_FILE_PARTITION_TIME_GRANULARITY_TYPE** to HOUR for hourly partitions or to DATE for date based partitioning. The timestamp field of the message which needs to be used for partitioning can be configured via **SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_FIELD_NAME** config.

#### Does the sink support S3 or hdfs ?
Though the sink only supports uploading files to Google Cloud Storage as of the latest release, other storage providers can be added as well.

#### How the schema being generated ? How the data types being converted to Parquet types ?
Schema of the Kafka message is read from the proto class specified via configs. Stencil is used to serialize the message byte array into a Protobuf Dynamic Message using this proto. It is then finally written to a parquet file. In case a stencil registry is configured, the schema changes will be dynamically updated.

#### How is the folder path for the file that being created ?
The filename is generated as a random UUID string. For the local file, base directory path is taken from the config  
**SINK_BLOB_LOCAL_DIRECTORY** and by default it is set to `/tmp/firehose`. Within this directory, subdirectories  
are generated with names based on whether partitioning is enabled or not:
1. If **SINK_BLOB_FILE_PARTITION_TIME_GRANULARITY_TYPE** is set to NONE meaning partitioning is disabled, the parquet files  
   created are placed in just one directory with name set to the topic name of the kafka messages.
2. If **SINK_BLOB_FILE_PARTITION_TIME_GRANULARITY_TYPE** is set to DAY, the parquet files generated are placed in directories  
   with names in the format [topic-name]/[date-prefix][date], where,  
   i. [topic-name] is the topic name of the Kafka message  
   ii. [date-prefix] is a config fetched from **SINK_BLOB_FILE_PARTITION_TIME_DATE_PREFIX** and is set to `dt=` as default,  
   iii. [date] is the date in `yyyy-MM-dd` format extracted from a timestamp column of the Kafka message as specified via  
   **SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_FIELD_NAME** and converted to the timezone of  
   **SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_TIMEZONE** (default UTC)
3. If **SINK_OBJECT_STORAGE_TIME_PARTITIONING_TYPE** is set to HOUR, the parquet files generated are placed in directories  
   with names in the format [topic-name]/[date-prefix][date]/[hour-prefix][hour], where,  
   i. [hour-prefix] is a config fetched from **SINK_BLOB_FILE_PARTITION_TIME_HOUR_PREFIX** and is set to `hr=` as default,  
   ii. [hour] is the hour in `HH` format extracted from a timestamp column of the Kafka message as specified via  
   **SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_FIELD_NAME** and converted to the timezone of  
   **SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_TIMEZONE**(default UTC).

#### How to configure object storage bucket ?
Firehose will automatically configure the GCS bucket upon starting by reading the credentials, project id, bucket name and other settings as configured via environnment variables. Please look at he GCS Sink configs for more details.

#### What will happen when file upload failed ?
In case the file upload fails, the thread processing the file upload throws an exception which ultimately bubbles up and causes the Firehose JVM to get terminated with an abnormal status code.

#### What will happen when disk is full ?
In case the file upload fails, an IOException is thrown which ultimately bubbles up and causes the Firehose JVM to get terminated with an abnormal status code.

#### How to implement new Object Storage provider, for example S3 ?
Firehose exposes an interface class called *BlobStorage.java* which can be used to create new blob storage implementations. Subsequently, the *BlobStorageFactory.java* can be modified to produce instance of this new blob storage provider, while injecting the necessary configs. Firehose also exposes a config called as **SINK_BLOB_STORAGE_TYPE** which is used to control which storage provider to use, with its default currently set to GCS.

#### How to implement new file format ?
Firehose exposes an interface called LocalFileWriter.java which can be used to create new file writer implementations. Subsequently, the LocalStorage.java class can be modified to produce instance of this new file writer, while injecting the necessary configs. Firehose also exposes a config called as SINK_BLOB_LOCAL_FILE_WRITER_TYPE which is used to control which file writer type to use, with its default currently set to parquet.

#### How to commit being handled ?
ObjectStorage sink does its own offset management. After the polled kafka messages are processed by the sink and the parquet files are uploaded to object storage and cleaned from local disk as per the policy, commitable offsets are then recorded by the sink. The offsets are then committed as per the Offset Manager. Please look at the Offset Manager class for more details.

#### How much Disk size recommended for the sink ?
A safe disk size will depend on and can be tuned based on configs set such as the time duration since creation after which files are pushed to the Blob Storage (**SINK_BLOB_LOCAL_FILE_ROTATION_DURATION_MS**), max parquet file size after which it is pushed to the Blob Storage(**SINK_BLOB_LOCAL_FILE_ROTATION_MAX_SIZE_BYTES**) as well as the number of topics that Firehose is consuming from.