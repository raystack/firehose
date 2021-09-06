# ElasticSearch Sink

An ES sink Firehose \(`SINK_TYPE`=`elasticsearch`\) requires the following variables to be set along with Generic ones

## `SINK_ES_CONNECTION_URLS`

Elastic search connection URL/URLs to connect. Multiple URLs could be given in a comma separated format.

* Example value: `localhost1:9200`
* Type: `required`

## `SINK_ES_INDEX_NAME`

The name of the index to which you want to write the documents. If it does not exist, it will be created.

* Example value: `sample_index`
* Type: `required`

## `SINK_ES_TYPE_NAME`

Defines the type name of the Document in Elasticsearch.

* Example value: `Customer`
* Type: `required`

## `SINK_ES_ID_FIELD`

The identifier field of the document in Elasticsearch. This should be the key of the field present in the message \(JSON or Protobuf\) and it has to be a unique, non-null field. So the value of this field in the message will be used as the ID of the document in Elasticsearch while writing the document.

* Example value: `customer_id`
* Type: `required`

## `SINK_ES_MODE_UPDATE_ONLY_ENABLE`

Elasticsearch sink can be created in 2 modes: `Upsert mode` or `UpdateOnly mode`. If this config is set:

* `TRUE`: Firehose will run on UpdateOnly mode which will only UPDATE the already existing documents in the Elasticsearch index.
* `FALSE`: Firehose will run on Upsert mode, UPDATING the existing documents and also INSERTING any new ones.
  * Example value: `TRUE`
  * Type: `required`
  * Default value: `FALSE`

## `SINK_ES_INPUT_MESSAGE_TYPE`

Indicates if the Kafka topic contains JSON or Protocol Buffer messages.

* Example value: `PROTOBUF`
* Type: `required`
* Default value: `JSON`

## `SINK_ES_PRESERVE_PROTO_FIELD_NAMES_ENABLE`

Whether or not the protobuf field names should be preserved in the Elasticsearch document. If false the fields will be converted to camel case.

* Example value: `FALSE`
* Type: `required`
* Default value: `TRUE`

## `SINK_ES_REQUEST_TIMEOUT_MS`

Defines the request timeout of the elastic search endpoint. The value specified is in milliseconds.

* Example value: `60000`
* Type: `required`
* Default value: `60000`

## `SINK_ES_SHARDS_ACTIVE_WAIT_COUNT`

Defines the number of shard copies that must be active before proceeding with the operation. This can be set to any non-negative value less than or equal to the total number of shard copies \(number of replicas + 1\).

* Example value: `1`
* Type: `required`
* Default value: `1`

## `SINK_ES_RETRY_STATUS_CODE_BLACKLIST`

List of comma-separated status codes for which Firehose should not retry in case of UPDATE ONLY mode is TRUE

* Example value: `404,400`
* Type: `optional`

## `SINK_ES_ROUTING_KEY_NAME`

Defines the proto field whose value will be used for routing documents to a particular shard in Elasticsearch. If empty, Elasticsearch uses the ID field of the doc by default.

* Example value: `service_type`
* Type: `optional`

