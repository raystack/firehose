# Templating

Firehose HTTP sink supports payload templating using [`SINK_HTTP_JSON_BODY_TEMPLATE`](../reference/configuration.md#-sink_http_json_body_template) configuration. It uses [JsonPath](https://github.com/json-path/JsonPath) for creating Templates which is a DSL for basic JSON parsing. Playground for this: [https://jsonpath.com/](https://jsonpath.com/), where users can play around with a given JSON to extract out the elements as required and validate the `jsonpath`. The template works only when the output data format [`SINK_HTTP_DATA_FORMAT`](../reference/configuration.md#-sink_http_data_format) is JSON.

_**Creating Templates:**_

This is really simple. Find the paths you need to extract using the JSON path. Create a valid JSON template with the static field names + the paths that need to extract. \(Paths name starts with $.\). Firehose will simply replace the paths with the actual data in the path of the message accordingly. Paths can also be used on keys, but be careful that the element in the key must be a string data type.

One sample configuration\(On XYZ proto\) : `{"test":"$.routes[0]", "$.order_number" : "xxx"}` If you want to dump the entire JSON as it is in the backend, use `"$._all_"` as a path.

Limitations:

* Works when the input DATA TYPE is a protobuf, not a JSON.
* Supports only on messages, not keys.
* validation on the level of valid JSON template. But after data has been replaced the resulting string may or may not be a valid JSON. Users must do proper testing/validation from the service side.
* If selecting fields from complex data types like repeated/messages/map of proto, the user must do filtering based first as selecting a field that does not exist would fail.

