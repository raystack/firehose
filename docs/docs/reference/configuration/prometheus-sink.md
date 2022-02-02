# Prometheus Sink

A Prometheus sink Firehose \(`SINK_TYPE`=`prometheus`\) requires the following variables to be set along with Generic ones.

## `SINK_PROM_SERVICE_URL`

Defines the HTTP/Cortex endpoint of the service to which this consumer should POST data.

* Example value: `http://localhost:9009/api/prom/push`
* Type: `required`

## `SINK_PROM_REQUEST_TIMEOUT_MS`

Defines the connection timeout for the request in millis.

* Example value: `10000`
* Type: `required`
* Default value: `10000`

## `SINK_PROM_RETRY_STATUS_CODE_RANGES`

Defines the range of HTTP status codes for which retry will be attempted.

* Example value: `400-600`
* Type: `optional`
* Default value: `400-600`

## `SINK_PROM_REQUEST_LOG_STATUS_CODE_RANGES`

Defines the range of HTTP status codes for which the request will be logged.

* Example value: `400-499`
* Type: `optional`
* Default value: `400-499`

## `SINK_PROM_HEADERS`

Defines the HTTP headers required to push the data to the above URL.

* Example value: `Authorization:auth_token, Accept:text/plain`
* Type: `optional`

## `SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING`

The mapping of fields and the corresponding proto index which will be set as the metric name on Cortex. This is a JSON field.

* Example value: `{"2":"tip_amount","1":"feedback_ratings"}`
  * Proto field value with index 2 will be stored as metric named `tip_amount` in Cortex and so on
* Type: `required`

## `SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING`

The mapping of proto fields to metric lables. This is a JSON field. Each metric defined in `SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING` will have all the labels defined here.

* Example value: `{"6":"customer_id"}`
* Type: `optional`

## `SINK_PROM_WITH_EVENT_TIMESTAMP`

If set to true, metric timestamp will using event timestamp otherwise it will using timestamp when Firehose push to endpoint.

* Example value: `false`
* Type: `optional`
* Default value: `false`

## `SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX`

Defines the proto index of a field that can be used as the timestamp.

* Example value: `2`
* Type: `required (if SINK_PROM_WITH_EVENT_TIMESTAMP=true)`

