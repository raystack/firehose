# Filters

Following variables need to be set to enable JSON/JEXL filters.

## `FILTER_ENGINE`

Defines whether to use JSON Schema-based filters or JEXL-based filters

* Example value: `JSON`
* Type: `optional`
* Default value`: JEXL`

## `FILTER_ESB_MESSAGE_TYPE`

Defines the format type of the input ESB messages, i.e. JSON/Protobuf.

* Example value: `JSON`
* Type: `optional`

## `FILTER_JEXL_DATA_SOURCE`

`key`/`message`/`none`depending on where to apply filter

* Example value: `key`
* Type: `optional`
* Default value`: none`

## `FILTER_JEXL_EXPRESSION`

JEXL filter expression

* Example value: `driverLocationLogKey.getVehicleType()=="BIKE"`
* Type: `optional`

## `FILTER_JEXL_SCHEMA_PROTO_CLASS`

The fully qualified name of the proto schema so that the key/message in Kafka could be parsed.

* Example value: `com.gojek.esb.driverlocation.DriverLocationLogKey`
* Type: `optional`

## `FILTER_JSON_DATA_SOURCE`

`key`/`message`/`none`depending on where to apply filter

* Example value: `key`
* Type: `optional`
* Default value`: none`

## `FILTER_JSON_SCHEMA_PROTO_CLASS`

The fully qualified name of the Proto schema so that the key/message in Kafka could be parsed.

* Example value: `com.example.driverlocation.DriverLocationLogKey`
* Type: `required if FILTER_ESB_MESSAGE_TYPE=PROTOBUF`

## `FILTER_JSON_SCHEMA`

JSON Schema string containing the filter rules to be applied.

* Example value: `{"properties":{"order_number":{"const":"1253"}}}`
* Type: `optional`

