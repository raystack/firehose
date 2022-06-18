# Decorators

Decorators implement the Sink interface, and they can wrap other sinks. Decorators can also wrap other decorators. They
process messages returned by the wrapped Sink. Sink and SinkDecorator pushMessage() API return messages that were not
successful.

## Type of Decorators 

This is the order of execution decorators after sink returns failed messages.
A failed message will have one of these error types:
* DESERIALIZATION_ERROR
* INVALID_MESSAGE_ERROR
* UNKNOWN_FIELDS_ERROR
* SINK_4XX_ERROR
* SINK_5XX_ERROR
* SINK_UNKNOWN_ERROR
* DEFAULT_ERROR

### SinkWithFailHandler

This decorator is intended to be used to trigger consumer failure based on configured error types.
Configuration `ERROR_TYPES_FOR_FAILING` is to be set with the comma separated list of Error types.

### SinkWithRetry

This decorator retries to push messages based on the configuration set for error types `ERROR_TYPES_FOR_RETRY`.
It will retry for the maximum of `RETRY_MAX_ATTEMPTS` with exponential back off.

### SinkWithDlq
This decorator pushes messages to DLQ based on the error types set in `ERROR_TYPES_FOR_DLQ`.
This decorator will only be added if `DLQ_SINK_ENABLE` is set to be true.
There are three types of DLQWriter that can be configured by setting `DLQ_WRITER_TYPE`.
#### Log
This is just for debugging, it prints out the messages to standard output.

#### Kafka
Based on the configurations in `DlqKafkaProducerConfig` class, messages are pushed to kafka.

#### Blob
Blob Storage can also be used to DLQ messages. Currently, only GCS is supported as a store.
The `BlobStorageDlqWriter` converts each message into json String, and appends multiple messages via new line. 
These messages are pushed to a blob storage. The object name for messages is`topic_name/consumed_timestamp/a-random-uuid`.

### SinkFinal

This decorator is the black hole for messages. The messages reached here are ignored
and no more processing is done on them. 
