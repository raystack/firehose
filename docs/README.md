#### Introduction
Firehose is a fully managed service for delivering real-time streaming data to destinations such as service endpoints (HTTP or GRPC) & managed databases (Postgres, InfluxDB,  Redis, & ElasticSearch). With Firehose, you don't need to write applications or manage resources. It automatically scales to match the throughput of your data and requires no ongoing administration. If your data is present in Kafka, Firehose delivers it to the destination(SINK) that you specified.

Supported Sinks:

* Log
* HTTP
* JDBC
* InfluxDB
* Redis
* ElasticSearch
* GRPC


## How to guides
#### Deploy your first Firehose
Coming Soon...
    
## Firehose VS Kafka-Connect
#### Kafka-Connect
PROS
* Can consume from Kafka as well as many other sources
* Easier to integrate if working with confluent-platform
* Provides a bunch of transformation options as part of Single Message Transformations(SMTs)

CONS
* When using in distributed mode across multiple nodes, need to install all the connectors across all the workers within your kafka connect cluster
* Available connectors may have some limitations. Its usually rare to find all the required features in a single connector and so is to find a documentation for the same
* Separation of commercial and open-source features is very poor
* For monitoring and many other features confluent control center asks for an enterprise subscription
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#### Firehose
PROS
* Easier to install and use different sinks by tweaking only a couple of configurations
* Comes with tons of exclusive features for each sink. e.g. JSON body template, Parameterised header/request, OAuth2 for HTTP sink
* Value based filtering is much easier to implement as compared to Kafka-Connect. Requires no additional plugins/schema-registry to be installed
* Provides a comprehensible Abstract sink contract which makes it Easier to add a new sink in Firehose
* Don't need to think about converters and serializers, Firehose comes with an inbuilt serialization/deserialization
* Comes with Firehose health dashboard (Grafana) for effortless monitoring free of cost

CONS
* Can consume from Kafka but not from any other data source
* Supports only protobuf format as of now
* Doesn't support Kafka Sink yet