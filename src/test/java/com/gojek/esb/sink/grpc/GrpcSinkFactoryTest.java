package com.gojek.esb.sink.grpc;


import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.agent.model.NewService;
import com.gojek.esb.consumer.Error;
import com.gojek.esb.consumer.*;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.Sink;
import com.google.protobuf.AbstractMessage;
import com.pszymczyk.consul.junit.ConsulResource;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.stubbing.Stubber;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class GrpcSinkFactoryTest {

    @ClassRule
    public static final ConsulResource CONSUL = new ConsulResource();

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private TestServerGrpc.TestServerImplBase testGrpcService;

    private static ConsulClient consulClient;

    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void shouldCreateChannelPoolWithServiceDiscovery() throws IOException, DeserializerException {
        when(testGrpcService.bindService()).thenCallRealMethod();

        Server server = ServerBuilder
                .forPort(5000)
                .addService(testGrpcService.bindService())
                .build()
                .start();
        NewService service = new NewService();
        service.setName("test-service");
        service.setAddress("localhost");
        service.setPort(server.getPort());
        service.setId("testID");
        consulClient = new ConsulClient("localhost", CONSUL.getHttpPort());
        consulClient.agentServiceRegister(service);


        Map<String, String> config = new HashMap<>();
        config.put("GRPC_METHOD_URL", "com.gojek.esb.consumer.TestServer/TestRpcMethod");
        config.put("CONSUL_SERVICE_DISCOVERY", "true");
        config.put("CONSUL_CLIENT_HOST", "localhost");
        config.put("CONSUL_CLIENT_PORT", String.valueOf(CONSUL.getHttpPort()));
        config.put("CONSUL_SERVICE_NAME", "test-service");

        TestGrpcRequest request = TestGrpcRequest.newBuilder().build();

        GrpcSinkFactory grpcSinkFactory = new GrpcSinkFactory();

        Sink sink = grpcSinkFactory.create(config, statsDReporter);


        EsbMessage esbMessage = new EsbMessage(null, request.toByteArray(), "some-topic", 10, 10);

        doAnswerProtoReponse(TestGrpcResponse.newBuilder()
                .setSuccess(true)
                .addError(Error.newBuilder().build())
                .build()).when(testGrpcService).testRpcMethod(any(TestGrpcRequest.class), any());

        sink.pushMessage(Collections.singletonList(esbMessage));

        verify(testGrpcService, times(1)).testRpcMethod(any(TestGrpcRequest.class), any());
        consulClient.agentServiceDeregister("testId");
        server.shutdownNow();
    }

    @Test
    public void shouldCreateChannelPoolWithHostAndPort() throws IOException, DeserializerException {
        when(testGrpcService.bindService()).thenCallRealMethod();

        Server server = ServerBuilder
                .forPort(5000)
                .addService(testGrpcService.bindService())
                .build()
                .start();

        Map<String, String> config = new HashMap<>();
        config.put("GRPC_METHOD_URL", "com.gojek.esb.consumer.TestServer/TestRpcMethod");
        config.put("SERVICE_HOST", "localhost");
        config.put("SERVICE_PORT", "5000");

        TestGrpcRequest request = TestGrpcRequest.newBuilder().build();

        GrpcSinkFactory grpcSinkFactory = new GrpcSinkFactory();

        Sink sink = grpcSinkFactory.create(config, statsDReporter);


        EsbMessage esbMessage = new EsbMessage(null, request.toByteArray(), "some-topic", 10, 10);

        doAnswerProtoReponse(TestGrpcResponse.newBuilder()
                .setSuccess(true)
                .addError(Error.newBuilder().build())
                .build()).when(testGrpcService).testRpcMethod(any(TestGrpcRequest.class), any());

        sink.pushMessage(Collections.singletonList(esbMessage));

        verify(testGrpcService, times(1)).testRpcMethod(any(TestGrpcRequest.class), any());
        server.shutdownNow();
    }

    private <T extends AbstractMessage> Stubber doAnswerProtoReponse(T response) {
        return doAnswer(invocation -> {
            StreamObserver<T> responseObserver = (StreamObserver<T>) invocation.getArguments()[1];
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return null;
        });
    }
}
