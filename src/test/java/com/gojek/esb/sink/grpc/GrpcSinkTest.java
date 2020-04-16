package com.gojek.esb.sink.grpc;


import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.grpc.response.GrpcResponse;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.grpc.client.GrpcClient;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;


public class GrpcSinkTest {

    private GrpcSink sink;

    @Mock
    private EsbMessage esbMessage;

    @Mock
    private GrpcClient grpcClient;

    @Mock
    private Instrumentation instrumentation;

    @Before
    public void setUp() {
        initMocks(this);
        sink = new GrpcSink(instrumentation, grpcClient);
    }

    @Test
    public void shouldWriteToSink() throws Exception {
        when(esbMessage.getLogMessage()).thenReturn(new byte[]{});
        RecordHeaders headers = new RecordHeaders();
        when(esbMessage.getHeaders()).thenReturn(headers);
        when(grpcClient.execute(any(byte[].class), any(RecordHeaders.class))).thenReturn(GrpcResponse.newBuilder().setSuccess(true).build());

        sink.pushMessage(Collections.singletonList(esbMessage));
        verify(grpcClient, times(1)).execute(any(byte[].class), eq(headers));
    }

    @Test
    public void shouldReturnBackListOfFailedMessages() throws IOException, DeserializerException {
        when(esbMessage.getLogMessage()).thenReturn(new byte[]{});
        when(esbMessage.getHeaders()).thenReturn(new RecordHeaders());
        when(grpcClient.execute(any(), any(RecordHeaders.class))).thenReturn(GrpcResponse.newBuilder().setSuccess(false).build());
        List<EsbMessage> failedMessages = sink.pushMessage(Collections.singletonList(esbMessage));

        assertFalse(failedMessages.isEmpty());
        assertEquals(1, failedMessages.size());
    }
}
