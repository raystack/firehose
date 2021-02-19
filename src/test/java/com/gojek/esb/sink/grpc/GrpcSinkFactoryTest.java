package com.gojek.esb.sink.grpc;


import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.consumer.TestServerGrpc;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.Sink;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class GrpcSinkFactoryTest {

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private TestServerGrpc.TestServerImplBase testGrpcService;

    @Mock
    private StencilClient stencilClient;

//    private static ConsulClient consulClient;

    @Before
    public void setUp() {
        initMocks(this);
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
        config.put("sink.grpc.method.url", "com.gojek.esb.consumer.TestServer/TestRpcMethod");
        config.put("sink.grpc.service.host", "localhost");
        config.put("sink.grpc.service.port", "5000");

        GrpcSinkFactory grpcSinkFactory = new GrpcSinkFactory();

        Sink sink = grpcSinkFactory.create(config, statsDReporter, stencilClient);

        Assert.assertNotNull(sink);
        server.shutdownNow();
    }
}
