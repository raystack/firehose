# Troubleshooting

### Consumer Lag

When it comes to decreasing the topic lag, it often helps to have the environment variable - [`SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS`](../reference/configuration.md#source_kafka_consumer_config_max_poll_records) config to be increased from the default of 500 to something higher.

Additionally, you can increase the workers in the Firehose which will effectively multiply the number of records being processed by Firehose. However, please be mindful of the caveat mentioned below.

#### The caveat to the aforementioned remedies: 

Be mindful of the fact that your sink also needs to be able to process this higher volume of data being pushed to it. Because if it is not, then this will only compound the problem of increasing lag.

Alternatively, if your underlying sink is not able to handle increased \(or default\) volume of data being pushed to it, adding some sort of a filter condition in the Firehose to ignore unnecessary messages in the topic would help you bring down the volume of data being processed by the sink.

