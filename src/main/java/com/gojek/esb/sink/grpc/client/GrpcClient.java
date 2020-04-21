package com.gojek.esb.sink.grpc.client;


import com.gojek.esb.config.GrpcConfig;
import com.gojek.esb.grpc.response.GrpcResponse;
import com.gopay.grpc.ChannelPool;
import com.newrelic.api.agent.Trace;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ClientInterceptors;
import io.grpc.CallOptions;
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

    private ChannelPool channelPool;
    private final GrpcConfig grpcConfig;

    public GrpcClient(ChannelPool channelPool, GrpcConfig grpcConfig) {
        this.channelPool = channelPool;
        this.grpcConfig = grpcConfig;
    }


    @Trace(dispatcher = true)
    public GrpcResponse execute(byte[] logMessage, Headers headers) {

        MethodDescriptor.Marshaller<byte[]> marshaller = getMarshaller();
        ManagedChannel managedChannel = null;
        GrpcResponse grpcResponse;

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
                    MethodDescriptor.create(
                            MethodDescriptor.MethodType.UNARY,
                            grpcConfig.getGrpcMethodUrl(),
                            marshaller,
                            marshaller),
                    CallOptions.DEFAULT,
                    logMessage);
            grpcResponse = GrpcResponse.parseFrom(response);

        } catch (Exception e) {
//            System.out.println("Failed to send request {} " + e.getMessage());
            grpcResponse = GrpcResponse
                    .newBuilder()
                    .setSuccess(false)
                    .build();
        } finally {
            if (managedChannel != null) {
                channelPool.returnObject(managedChannel);
            }
        }
        return grpcResponse;
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
