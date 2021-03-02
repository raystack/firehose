# Configuration Reference

This page contains reference for all the configurations for Firehose.

| VARIABLE NAME                    | DESCRIPTION                                       | NECESSITY | SAMPLE VALUE            |
|----------------------------------|---------------------------------------------------|-----------|-------------------------|
| `source.kafka.brokers`           | list of kafka brokers to consume from             | Mandatory | `localhost:9092`        |
| `source.kafka.topic`             | list of kafka topics to consume from              | Mandatory | `test-topic`            |
| `source.kafka.consumer.group.id` | Kafka consumer group ID                           | Mandatory | `sample-group-id`       |
| `kafka.record.parcer.mode`       | Decides whether to parse key or message           | Mandatory | `message`               |
| `sink.type`                      | Firehose sink type                                | Mandatory | `log`                   |
| `input.schema.proto.class`       | The fully qualified name of the input proto class | Mandatory | `com.tests.TestMessage` |

