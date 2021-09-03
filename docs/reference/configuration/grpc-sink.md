# GRPC Sink

A GRPC sink Firehose \(`SINK_TYPE`=`grpc`\) requires the following variables to be set along with Generic ones

## `SINK_GRPC_SERVICE_HOST`

Defines the host of the GRPC service.

* Example value: `http://grpc-service.sample.io`
* Type: `required`

## `SINK_GRPC_SERVICE_PORT`

Defines the port of the GRPC service.

* Example value: `8500`
* Type: `required`

## `SINK_GRPC_METHOD_URL`

Defines the URL of the GRPC method that needs to be called.

* Example value: `com.tests.SampleServer/SomeMethod`
* Type: `required`

## `SINK_GRPC_RESPONSE_SCHEMA_PROTO_CLASS`

Defines the Proto which would be the response of the GRPC Method.

* Example value: `com.tests.SampleGrpcResponse`
* Type: `required`

