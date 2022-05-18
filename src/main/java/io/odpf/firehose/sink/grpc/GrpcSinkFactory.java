package io.odpf.firehose.sink.grpc;


import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.firehose.config.GrpcSinkConfig;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.grpc.client.GrpcClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.odpf.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the GrpcSink.
 * <p>
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient client)}
 * to obtain the GrpcSink sink implementation.
 */
public class GrpcSinkFactory {

    public static AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        GrpcSinkConfig grpcConfig = ConfigFactory.create(GrpcSinkConfig.class, configuration);
        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, GrpcSinkFactory.class);
        String grpcSinkConfig = String.format("\n\tService host: %s\n\tService port: %s\n\tMethod url: %s\n\tResponse proto schema: %s",
                grpcConfig.getSinkGrpcServiceHost(), grpcConfig.getSinkGrpcServicePort(), grpcConfig.getSinkGrpcMethodUrl(), grpcConfig.getSinkGrpcResponseSchemaProtoClass());
        firehoseInstrumentation.logDebug(grpcSinkConfig);

        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress(grpcConfig.getSinkGrpcServiceHost(), grpcConfig.getSinkGrpcServicePort()).usePlaintext().build();

        GrpcClient grpcClient = new GrpcClient(new FirehoseInstrumentation(statsDReporter, GrpcClient.class), grpcConfig, managedChannel, stencilClient);
        firehoseInstrumentation.logInfo("GRPC connection established");

        return new GrpcSink(new FirehoseInstrumentation(statsDReporter, GrpcSink.class), grpcClient, stencilClient);
    }

}
