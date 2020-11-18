package com.gojek.esb.sink.grpc;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.config.GrpcConfig;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.SinkFactory;
import com.gojek.esb.sink.grpc.client.GrpcClient;
import com.gopay.grpc.ChannelPool;
import com.gopay.grpc.ChannelPoolException;
import com.gopay.grpc.ConsulChannelPool;
import io.grpc.netty.NegotiationType;
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
                        + "\n\tConnection pool max wait millis: %s\n\tConsul overloaded threshold: %s\n\tConsul service name: %s",
                grpcConfig.getServiceHost(), grpcConfig.getServicePort(), grpcConfig.getGrpcMethodUrl(), grpcConfig.getGrpcResponseProtoSchema(),
                grpcConfig.getConnectionPoolMaxIdle(), grpcConfig.getConnectionPoolMinIdle(), grpcConfig.getConnectionPoolSize(),
                grpcConfig.getConnectionPoolMaxWaitMillis(), grpcConfig.getConsulOverloadedThreshold(), grpcConfig.getConsulServiceName());
        instrumentation.logDebug(grpcSinkConfig);
        instrumentation.logInfo("Consul service discovery enabled: {}", grpcConfig.getConsulServiceDiscovery());

        ChannelPool connection = null;
        try {
            connection = createConnection(grpcConfig);
        } catch (ChannelPoolException e) {
            instrumentation.logError(e.getMessage());
            e.printStackTrace();
        }

        GrpcClient grpcClient = new GrpcClient(new Instrumentation(statsDReporter, GrpcClient.class), connection, grpcConfig, stencilClient);
        instrumentation.logInfo("GRPC connection established");

        return new GrpcSink(new Instrumentation(statsDReporter, GrpcSink.class), grpcClient, stencilClient);
    }

    private ChannelPool createConnection(GrpcConfig configuration) throws ChannelPoolException {
        if (configuration.getConsulServiceDiscovery()) {
            return getChannelPoolWithServiceDiscovery(configuration);
        }

        return ChannelPool.create(configuration.getServiceHost(),
                configuration.getServicePort(),
                configuration.getConnectionPoolSize(),
                configuration.getConnectionPoolMaxIdle(),
                configuration.getConnectionPoolMinIdle(),
                "grpc-pool",
                NegotiationType.PLAINTEXT);
    }

    private ChannelPool getChannelPoolWithServiceDiscovery(GrpcConfig configuration) throws ChannelPoolException {
            return ConsulChannelPool.create(configuration.getServiceHost(),
                    configuration.getServicePort(),
                    configuration.getConsulServiceName(),
                    configuration.getConnectionPoolSize(),
                    configuration.getConnectionPoolMaxIdle(),
                    configuration.getConnectionPoolMinIdle(),
                    configuration.getConsulOverloadedThreshold(),
                    "grpc-pool");
        }

}
