# InfluxDB Sink

An Influx sink Firehose \(`SINK_TYPE`=`influxdb`\) requires the following variables to be set along with Generic ones

## `SINK_INFLUX_URL`

InfluxDB URL, it's usually the hostname followed by port.

* Example value: `http://localhost:8086`
* Type: `required`

## `SINK_INFLUX_USERNAME`

Defines the username to connect to DB.

* Example value: `root`
* Type: `required`

## `SINK_INFLUX_PASSWORD`

Defines the password to connect to DB.

* Example value: `root`
* Type: `required`

## `SINK_INFLUX_FIELD_NAME_PROTO_INDEX_MAPPING`

Defines the mapping of fields and the corresponding proto index which can be used to extract the field value from the proto message. This is a JSON field. Note that Influx keeps a single value for each unique set of tags and timestamps. If a new value comes with the same tag and timestamp from the source, it will override the existing one.

* Example value: `"2":"order_number","1":"service_type", "4":"status"`
  * Proto field value with index 2 will be stored in a field named 'order\_number' in Influx and so on\
* Type: `required`

## `SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING`

Defines the mapping of tags and the corresponding proto index from which the value for the tags can be obtained. If the tags contain existing fields from field name mapping it will not be overridden. They will be duplicated. If ENUMS are present then they must be added here. This is a JSON field.

* Example value: `{"6":"customer_id"}`
* Type: `optional`

## `SINK_INFLUX_PROTO_EVENT_TIMESTAMP_INDEX`

Defines the proto index of a field that can be used as the timestamp.

* Example value: `5`
* Type: `required`

## `SINK_INFLUX_DB_NAME`

Defines the InfluxDB database name where data will be dumped.

* Example value: `status`
* Type: `required`

## `SINK_INFLUX_RETENTION_POLICY`

Defines the retention policy for influx database.

* Example value: `quarterly`
* Type: `required`
* Default value: `autogen`

## `SINK_INFLUX_MEASUREMENT_NAME`

This field is used to give away the name of the measurement that needs to be used by the sink. Measurement is another name for tables and it will be auto-created if does not exist at the time Firehose pushes the data to the influx.

* Example value: `customer-booking`
* Type: `required`

