# Errors

These errors are returned by sinks. One can configure to which errors should be processed by which decorator. The error
type are:

* DESERIALIZATION_ERROR
* INVALID_MESSAGE_ERROR
* UNKNOWN_FIELDS_ERROR
* SINK_4XX_ERROR
* SINK_5XX_ERROR
* SINK_UNKNOWN_ERROR
* DEFAULT_ERROR 
  * If no error is specified

## `ERROR_TYPES_FOR_FAILING`

* Example value: `DEFAULT_ERROR,SINK_UNKNOWN_ERROR`
* Type: `optional`
* Default value: `DESERIALIZATION_ERROR,INVALID_MESSAGE_ERROR,UNKNOWN_FIELDS_ERROR`

## `ERROR_TYPES_FOR_DLQ`

* Example value: `DEFAULT_ERROR,SINK_UNKNOWN_ERROR`
* Type: `optional`
* Default value: ``

## `ERROR_TYPES_FOR_RETRY`

* Example value: `DEFAULT_ERROR`
* Type: `optional`
* Default value: `DEFAULT_ERROR`
