# Filters

## Filtering Messages

When you are consuming from a topic that collects data from multiple publishers and you are concerned with only a particular subset of that data, in that case, you don’t need to consume all of it from the topic.

Instead, use this Filter feature provided in Firehose which allows you to apply any filters on the fields present in the key or message of the event data set and helps you narrow it down to your use case-specific data.

However, understand that you are ignoring some data/messages and if that ignored value is important for your sink, you might lose/skip data that is essential. So it is best to verify that your filter condition is not skipping essential data, for example, payment transactions.

Firehose uses the Apache Commons JEXL library for parsing the filter expressions. Refer to the [Using Filters](../guides/json-based-filters.md) section for details on how to configure filters.

## JEXL - based Filtering

JEXL \(Java EXpressions Language\) is an Apache Commons library intended to facilitate the implementation of dynamic and scripting features in applications and frameworks written in Java. Its goal is to expose scripting features usable by technical operatives or consultants working with enterprise platforms.

The API and the expression language exploit Java-beans naming patterns through introspection to expose property getters and setters. It also considers public class fields as properties and allows to invoke any accessible method. JEXL-based filters can be applied to only Protobuf messages, and not on JSON messages.

To evaluate expressions using JEXL, you need three things:

- An [engine](https://commons.apache.org/jexl/apidocs/org/apache/commons/jexl3/JexlEngine.html) to create expressions,
- A [context](https://commons.apache.org/jexl/apidocs/org/apache/commons/jexl3/JexlContext.html) containing any variables, and
- An [expression](https://commons.apache.org/jexl/apidocs/org/apache/commons/jexl3/Expression.html)

Read more about Apache Commons JEXL project [here](https://commons.apache.org/proper/commons-jexl/index.html).

### How JEXL-based Filters Work

The filtering occurs in the following steps -

- Firehose Consumer creates a Filter object and initializes it with the values of -`FILTER_DATA_SOURCE` i.e. key/message,`FILTER_JEXL_EXPRESSION` and `FILTER_SCHEMA_PROTO_CLASS`as configured in the environment variables.
- `JexlFilter` iterates over the input List of events. For each event, the Protobuf Class as specified by the environment variable `FILTER_SCHEMA_PROTO_CLASS` , converts the key/message of the event from raw byte array to a POJO \(Plain Old Java Object\), which contains getters for accessing various fields of the event data.
- A`JEXLContext` is created to link the key/message proto reference in the JEXL expression with the POJO object generated earlier. JEXL engine converts `FILTER_JEXL_EXPRESSION` string into `JEXLExpression` object and replaces all occurrences of references in the string by the generated POJO object. `JEXLException` is thrown if the filter expression is invalid.
- The `JEXLExpression` is then evaluated for each of these parsed events. The messages for which the `JEXLExpression` evaluates to `true`, are added to the output List of messages and returned by the Filter.

## JSON - based Filtering

**JSON-based** filtering uses a JSON Schema string to apply filter rules and validate if the JSON message passes all filter checks. **JSON Schema** is a vocabulary that allows you to **annotate** and **validate** JSON documents. JSON Schema is a JSON media type for defining the structure of JSON data. It provides a contract for what JSON data is required for a given application and how to interact with it.

To filter messages using JSON-based filters, you need two things:

- A JSON Schema string, containing the filter rules
- A JSON Schema Validator, to validate the message against the JSON Schema

You can read more about JSON Schema [here](https://json-schema.org/). For more details on JSON Schema syntax and specifications, refer [this](https://json-schema.org/specification.html) article. The JSON Schema Validator library used in Firehose is [networknt/json-schema-validator](https://github.com/networknt/json-schema-validator) . JSON-based filters can be applied to both JSON and Protobuf messages.

### How JSON-based Filters Work

The filtering occurs in the following steps -

- JSON filter configurations are validated and logged to instrumentation by JsonFilterUtil. In case any configuration is invalid, then IllegalArgumentException is thrown and Firehose is terminated.
- If `FILTER_ESB_MESSAGE_FORMAT=PROTOBUF`, then the serialized key/message protobuf byte array is deserialized to POJO object by the Proto schema class. It is then converted to a JSON string so that it can be parsed by the JSON Schema Validator.
- If`FILTER_ESB_MESSAGE_FORMAT=JSON`, then the serialized JSON byte array is deserialized to a JSON message string.
- The JSON Schema validator performs a validation on the JSON message against the filter rules specified in the JSON Schema string provided in the environment variable`FILTER_JSON_SCHEMA.`
- If there are any validation errors, then that key/message is filtered out and the validation errors are logged to the instrumentation in debug mode.
- If all validation checks pass, then the key/message is added to the ArrayList of filtered messages and returned by the JsonFilter.

## Why Use Filters

Filters enable you to consume only a smaller subset of incoming messages fulfilling a particular set of criteria while discarding other messages. This is helpful in cases like for e.g.- processing the status of drivers riding a bike, obtaining data of drivers within a particular city, etc.

Additionally, Filters can also help to significantly decrease consumer lag when the rate of incoming messages is too high, thus providing significant performance improvements for your sink.

If your underlying sink is not able to handle increased \(or default\) volume of data being pushed to it, adding a filter condition in the Firehose to ignore unnecessary messages in the topic would help you bring down the volume of data being processed by the sink.
