# Configurations

This page contains reference for all the application configurations for Firehose.

### <a name="Generic" /> Generic
A log sink firehose requires the following variables to be set

#### <a name="source.kafka.brokers" /> `source.kafka.brokers` 

Example value: `localhost:9092`\
Type: `required`

Sets the bootstrap server of kafka brokers to consume from.

#### <a name="source.kafka.topic" /> `source.kafka.topic` 

Example value: `test-topic`\
Type: `required`

List of kafka topics to consume from    

#### <a name="source.kafka.consumer.group.id" /> `source.kafka.consumer.group.id` 

Example value: `sample-group-id`\
Type: `required`

Kafka consumer group ID .

#### <a name="kafka.record.parser.mode" /> `kafka.record.parser.mode` 

Example value: `message`\
Type: `required`

Decides whether to parse key or message

#### <a name="sink.type" /> `sink.type` 

Example value: `'localhost:9092'`\
Type: `required`

Firehose sink type 

#### <a name="input.schema.proto.class" /> `input.schema.proto.class` 

Example value: `com.tests.TestMessage`\
Type: `required`

The fully qualified name of the input proto class 

