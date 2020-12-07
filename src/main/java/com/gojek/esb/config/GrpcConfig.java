package com.gojek.esb.config;

import org.aeonbits.owner.Config;


public interface GrpcConfig extends AppConfig {

    @Config.Key("GRPC_SERVICE_HOST")
    String getServiceHost();

    @Config.Key("GRPC_SERVICE_PORT")
    Integer getServicePort();

    @Config.Key("CONNECTION_POOL_SIZE")
    @Config.DefaultValue("1")
    Integer getConnectionPoolSize();

    @Config.Key("GRPC_METHOD_URL")
    String getGrpcMethodUrl();

    @Config.Key("CONNECTION_POOL_MAX_IDLE")
    @Config.DefaultValue("1")
    Integer getConnectionPoolMaxIdle();

    @Config.Key("CONNECTION_POOL_MIN_IDLE")
    @Config.DefaultValue("1")
    Integer getConnectionPoolMinIdle();

    @Config.Key("CONNECTION_POOL_MAX_WAIT_MILLIS")
    @Config.DefaultValue("500")
    Integer getConnectionPoolMaxWaitMillis();

    @Config.Key("GRPC_RESPONSE_PROTO_SCHEMA")
    @Config.DefaultValue("com.gojek.esb.de.meta.GrpcResponse")
    String getGrpcResponseProtoSchema();

}
