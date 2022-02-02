# Firehose Consumer

There are two type of consumer that can be configured.
`SOURCE_KAFKA_CONSUMER_MODE` can be set as `SYNC` or `ASYNC`.
SyncConsumer run in one thread on the other hand AsyncConsumer 
has a SinkPool. SinkPool can be configured by setting `SINK_POOL_NUM_THREADS`.
## FirehoseSyncConsumer

* Pull messages from kafka in batches.
* Apply filter based on filter configuration
* Add offsets of Not filtered messages into OffsetManager and set them committable.
* call sink.pushMessages() with filtered messages.
* Add offsets for remaining messages and set them committable.
* Call consumer.commit()
* Repeat.

## FirehoseAsyncConsumer
* Pull messages from kafka in batches.
* Apply filter based on filter configuration
* Add offsets of Not filtered messages into OffsetManager and set them committable.
* Schedule a task on SinkPool for these messages.
* Add offsets of these messages with key as the returned `Future`,
* Check SinkPool for finished tasks.
* Set offsets to be committable for any finished future. 
* Call consumer.commit()
* Repeat.


