package com.gojek.esb.config;

import org.aeonbits.owner.Config;


public interface GrpcSinkConfig extends AppConfig {

    @Config.Key("sink.grpc.service.host")
    String getSinkGrpcServiceHost();

    @Config.Key("sink.grpc.service.port")
    Integer getSinkGrpcServicePort();

    @Config.Key("sink.grpc.connection.pool.size")
    @Config.DefaultValue("1")
    Integer getSinkGrpcConnectionPoolSize();

    @Config.Key("sink.grpc.method.url")
    String getSinkGrpcMethodUrl();

    @Config.Key("sink.grpc.connection.pool.max.idle")
    @Config.DefaultValue("1")
    Integer getSinkGrpcConnectionPoolMaxIdle();

    @Config.Key("sink.grpc.connection.pool.min.idle")
    @Config.DefaultValue("1")
    Integer getSinkGrpcConnectionPoolMinIdle();

    @Config.Key("sink.grpc.connection.pool.max.wait.ms")
    @Config.DefaultValue("500")
    Integer getSinkGrpcConnectionPoolMaxWaitMs();

    @Config.Key("sink.grpc.response.proto.schema")
    @Config.DefaultValue("com.gojek.esb.de.meta.GrpcResponse")
    String getSinkGrpcResponseProtoSchema();

}
