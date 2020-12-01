package com.gojek.esb.sink.grpc.client;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.GrpcConfig;
import com.gojek.esb.metrics.Instrumentation;
import com.google.protobuf.DynamicMessage;
import com.gopay.grpc.ChannelPool;
import com.newrelic.api.agent.Trace;
import io.grpc.CallOptions;
import io.grpc.ClientInterceptors;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;


public class GrpcClient {

    private Instrumentation instrumentation;
    private ChannelPool channelPool;
    private final GrpcConfig grpcConfig;
    private ProtoParser protoParser;
    private  StencilClient stencilClient;

    public GrpcClient(Instrumentation instrumentation, ChannelPool channelPool, GrpcConfig grpcConfig, StencilClient stencilClient) {
        this.instrumentation = instrumentation;
        this.channelPool = channelPool;
        this.grpcConfig = grpcConfig;
        this.protoParser = new ProtoParser(stencilClient, grpcConfig.getGrpcResponseProtoSchema());
        this.stencilClient = stencilClient;
    }

    @Trace(dispatcher = true)
    public DynamicMessage execute(byte[] logMessage, Headers headers) {

        MethodDescriptor.Marshaller<byte[]> marshaller = getMarshaller();
        ManagedChannel managedChannel = null;
        DynamicMessage dynamicMessage;

        try {
            if (channelPool == null) {
                throw new IllegalStateException("ConnectionPool was not initiated successfully");
            }
            managedChannel = channelPool.borrowObject(grpcConfig.getConnectionPoolMaxWaitMillis());

            Metadata metadata = new Metadata();
            for (Header header : headers) {
                metadata.put(Metadata.Key.of(header.key(), Metadata.ASCII_STRING_MARSHALLER), new String(header.value()));
            }

            Channel decoratedChannel = ClientInterceptors.intercept(managedChannel,
                     MetadataUtils.newAttachHeadersInterceptor(metadata));
            byte[] response = ClientCalls.blockingUnaryCall(
                    decoratedChannel,
                    MethodDescriptor.newBuilder(marshaller, marshaller)
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(grpcConfig.getGrpcMethodUrl())
                            .build(),
                    CallOptions.DEFAULT,
                    logMessage);

            dynamicMessage = protoParser.parse(response);

        } catch (Exception e) {
            instrumentation.logWarn(e.getMessage());
            dynamicMessage = DynamicMessage.newBuilder(this.stencilClient.get(this.grpcConfig.getGrpcResponseProtoSchema())).build();

        } finally {
            if (managedChannel != null) {
                channelPool.returnObject(managedChannel);
            }
        }
        return dynamicMessage;
    }

    private MethodDescriptor.Marshaller<byte[]> getMarshaller() {
        return new MethodDescriptor.Marshaller<byte[]>() {
            @Override
            public InputStream stream(byte[] value) {
                return new ByteArrayInputStream(value);
            }

            @Override
            public byte[] parse(InputStream stream) {
                try {
                    return IOUtils.toByteArray(stream);
                } catch (IOException e) {
                    return null;
                }
            }
        };
    }

}
