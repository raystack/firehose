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

Bigquery sink \(`SINK_TYPE`=`bigquery`\) requires env variables to be set along with Generic ones and 
env variables in depot repository. The Firehose sink uses bigquery implementation available [depot](https://github.com/odpf/depot) repository. 

[Configuration of Bigquery Sink] (https://github.com/odpf/depot/blob/main/docs/reference/configuration/bigquery-sink.md)
