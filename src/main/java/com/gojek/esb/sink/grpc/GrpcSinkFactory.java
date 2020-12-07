package com.gojek.esb.sink.grpc;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.config.GrpcConfig;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.SinkFactory;
import com.gojek.esb.sink.grpc.client.GrpcClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the GrpcSink.
 * <p>
 * The esb-log-consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient client)}
 * to obtain the GrpcSink sink implementation.
 */

public class GrpcSinkFactory implements SinkFactory {

    @Override
    public AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        GrpcConfig grpcConfig = ConfigFactory.create(GrpcConfig.class, configuration);
        Instrumentation instrumentation = new Instrumentation(statsDReporter, GrpcSinkFactory.class);
        String grpcSinkConfig = String.format("\n\tService host: %s\n\tService port: %s\n\tMethod url: %s\n\tResponse proto schema: %s"
                        + "\n\tConnection pool max idle: %s\n\tConnection pool min idle: %s\n\tConnection poll size: %s"
                        + "\n\tConnection pool max wait millis: %s",
                grpcConfig.getServiceHost(), grpcConfig.getServicePort(), grpcConfig.getGrpcMethodUrl(), grpcConfig.getGrpcResponseProtoSchema(),
                grpcConfig.getConnectionPoolMaxIdle(), grpcConfig.getConnectionPoolMinIdle(), grpcConfig.getConnectionPoolSize(),
                grpcConfig.getConnectionPoolMaxWaitMillis());
        instrumentation.logDebug(grpcSinkConfig);

        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress(grpcConfig.getServiceHost(), grpcConfig.getServicePort()).usePlaintext().build();

        GrpcClient grpcClient = new GrpcClient(new Instrumentation(statsDReporter, GrpcClient.class), grpcConfig, managedChannel, stencilClient);
        instrumentation.logInfo("GRPC connection established");

        return new GrpcSink(new Instrumentation(statsDReporter, GrpcSink.class), grpcClient, stencilClient);
    }

}
