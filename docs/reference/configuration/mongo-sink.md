# MongoDB Sink

A MongoDB sink Firehose \(`SINK_TYPE`= `mongodb` \) requires the following variables to be set along with Generic ones

## `SINK_MONGO_CONNECTION_URLS`

MongoDB connection URL/URLs to connect. Multiple URLs could be given in a comma separated format.

* Example value: `localhost:27017`
* Type: `required`

## `SINK_MONGO_DB_NAME`

The name of the Mongo database to which you want to write the documents. If it does not exist, it will be created.

* Example value: `sample_DB`
* Type: `required`

## `SINK_MONGO_AUTH_ENABLE`

This field should be set to `true` if login authentication is enabled for the MongoDB Server.

* Example value: `true`
* Type: `optional`
* Default value: `false`

## `SINK_MONGO_AUTH_USERNAME`

The login username for session authentication to the MongoDB Server. This is a required field is `SINK_MONGO_AUTH_ENABLE` is set to `true`

* Example value: `bruce_wills`
* Type: `optional`

## `SINK_MONGO_AUTH_PASSWORD`

The login password for session authentication to the MongoDB Server. This is a required field is `SINK_MONGO_AUTH_ENABLE` is set to `true`

* Example value: `password@123`
* Type: `optional`

## `SINK_MONGO_AUTH_DB`

The name of the Mongo authentication database in which the user credentials are stored. This is a required field is `SINK_MONGO_AUTH_ENABLE` is set to `true`

* Example value: `sample_auth_DB`
* Type: `optional`

## `SINK_MONGO_COLLECTION_NAME`

Defines the name of the Mongo Collection

* Example value: `customerCollection`
* Type: `required`

## `SINK_MONGO_PRIMARY_KEY`

The identifier field of the document in MongoDB. This should be the key of a field present in the message \(JSON or Protobuf\) and it has to be a unique, non-null field. So the value of this field in the message will be copied to the `_id` field of the document in MongoDB while writing the document.

Note - If this parameter is not specified in Upsert mode \( i.e. when the variable`SINK_MONGO_MODE_UPDATE_ONLY_ENABLE=false`\), then Mongo server will assign the default UUID to the `_id` field, and only insert operations can be performed.

Note - this variable is a required field in the case of Update-Only mode \( i.e. when the variable`SINK_MONGO_MODE_UPDATE_ONLY_ENABLE=true`\). Also, all externally-fed documents must have this key copied to the `_id` field, for update operations to execute normally.

* Example value: `customer_id`
* Type: `optional`

## `SINK_MONGO_MODE_UPDATE_ONLY_ENABLE`

MongoDB sink can be created in 2 modes: `Upsert mode` or `UpdateOnly mode`. If this config is set:

* `TRUE`: Firehose will run on UpdateOnly mode which will only UPDATE the already existing documents in the MongoDB collection.
* `FALSE`: Firehose will run on Upsert mode, UPDATING the existing documents and also INSERTING any new ones.
  * Example value: `TRUE`
  * Type: `required`
  * Default value: `FALSE`

## `SINK_MONGO_INPUT_MESSAGE_TYPE`

Indicates if the Kafka topic contains JSON or Protocol Buffer messages.

* Example value: `PROTOBUF`
* Type: `required`
* Default value: `JSON`

## `SINK_MONGO_CONNECT_TIMEOUT_MS`

Defines the connect timeout of the MongoDB endpoint. The value specified is in milliseconds.

* Example value: `60000`
* Type: `required`
* Default value: `60000`

## `SINK_MONGO_RETRY_STATUS_CODE_BLACKLIST`

List of comma-separated status codes for which Firehose should not retry in case of UPDATE ONLY mode is TRUE

* Example value: `16608,11000`
* Type: `optional`

## `SINK_MONGO_PRESERVE_PROTO_FIELD_NAMES_ENABLE`

Whether or not the protobuf field names should be preserved in the MongoDB document. If false the fields will be converted to camel case.

* Example value: `false`
* Type: `optional`
* Default: `true`

## `SINK_MONGO_SERVER_SELECT_TIMEOUT_MS`

Sets the server selection timeout in milliseconds, which defines how long the driver will wait for server selection to succeed before throwing an exception. A value of 0 means that it will timeout immediately if no server is available. A negative value means to wait indefinitely.

* Example value: `4000`
* Type: `optional`
* Default: `30000`

