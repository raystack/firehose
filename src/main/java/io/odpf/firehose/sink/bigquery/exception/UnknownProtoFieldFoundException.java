package io.odpf.firehose.sink.bigquery.exception;

public class UnknownProtoFieldFoundException extends RuntimeException {
    public UnknownProtoFieldFoundException(String serializedUnknownFields, String serializedMessage) {
        super(String.format("[%s] unknown fields found in proto [%s], either update mapped protobuf or disable FAIL_ON_UNKNOWN_FIELDS",
                serializedUnknownFields, serializedMessage));
    }
}
