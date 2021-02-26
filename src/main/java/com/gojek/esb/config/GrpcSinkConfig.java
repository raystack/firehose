package com.gojek.esb.config;

import org.aeonbits.owner.Config;


public interface GrpcSinkConfig extends AppConfig {

    @Config.Key("sink.grpc.service.host")
    String getSinkGrpcServiceHost();

    @Config.Key("sink.grpc.service.port")
    Integer getSinkGrpcServicePort();

    @Config.Key("sink.grpc.method.url")
    String getSinkGrpcMethodUrl();

    @Config.Key("sink.grpc.response.schema.proto.class")
    String getSinkGrpcResponseSchemaProtoClass();

}
