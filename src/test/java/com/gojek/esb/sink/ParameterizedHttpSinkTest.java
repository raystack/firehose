//package com.gojek.esb.sink;
//
//import com.gojek.de.stencil.client.StencilClient;
//import com.gojek.esb.consumer.EsbMessage;
//import com.gojek.esb.exception.DeserializerException;
//import com.gojek.esb.sink.http.ParameterizedHttpSink;
//import com.gojek.esb.sink.http.client.ParameterizedHttpSinkClient;
//import org.apache.http.HttpResponse;
//import org.apache.http.StatusLine;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.Mockito;
//import org.mockito.runners.MockitoJUnitRunner;
//
//import java.io.IOException;
//import java.net.URISyntaxException;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//
//import static org.junit.Assert.assertEquals;
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.Mockito.doThrow;
//import static org.mockito.Mockito.when;
//
//@RunWith(MockitoJUnitRunner.class)
//public class ParameterizedHttpSinkTest {
//    @Mock
//    private EsbMessage esbMessage;
//
//    @Mock
//    private ParameterizedHttpSinkClient client;
//
//    @Mock
//    private HttpResponse httpResponse;
//
//    @Mock
//    private StatusLine statusLine;
//
//    @Mock
//    private StencilClient stencilClient;
//
//    @Before
//    public void setUp() throws DeserializerException, URISyntaxException {
//        Mockito.when(client.execute(Mockito.any())).thenReturn(httpResponse);
//        when(statusLine.getStatusCode()).thenReturn(200);
//        when(httpResponse.getStatusLine()).thenReturn(statusLine);
//    }
//
//    @Test
//    public void shouldSendSingleMessageIfParameterizedSink() throws IOException, DeserializerException, URISyntaxException {
//        Mockito.when(client.execute(any(EsbMessage.class))).thenReturn(null);
//        HashMap<Integer, Boolean> retryStatusCodeRanger = new HashMap<>();
//        retryStatusCodeRanger.put(500, true);
//        ParameterizedHttpSink sink = new ParameterizedHttpSink(client, retryStatusCodeRanger, stencilClient);
//        sink.pushMessage(Collections.singletonList(esbMessage));
//
//        Mockito.verify(client, Mockito.times(1)).execute(any(EsbMessage.class));
//    }
//
//    @Test
//    public void shouldReturnListOfFailedMessagesWhenErrorResponseForParameterizedSink() throws IOException, DeserializerException, URISyntaxException {
//        Mockito.when(client.execute(any(EsbMessage.class))).thenReturn(httpResponse);
//        when(statusLine.getStatusCode()).thenReturn(500);
//        when(httpResponse.getStatusLine()).thenReturn(statusLine);
//
//        HashMap<Integer, Boolean> retryStatusCodeRanger = new HashMap<>();
//        retryStatusCodeRanger.put(500, true);
//        ParameterizedHttpSink sink = new ParameterizedHttpSink(client, retryStatusCodeRanger, stencilClient);
//
//        List<EsbMessage> failedMessages = sink.pushMessage(Collections.singletonList(esbMessage));
//
//        Mockito.verify(client, Mockito.times(1)).execute(any(EsbMessage.class));
//        assertEquals(1, failedMessages.size());
//    }
//
//    @Test(expected = IOException.class)
//    public void shouldThrowExceptionWhenClientThrowsUriExceptionForParameterizedSink() throws URISyntaxException, IOException, DeserializerException {
//        doThrow(new URISyntaxException("Invalid Uri", "Uri")).when(client).execute(any(EsbMessage.class));
//        HashMap<Integer, Boolean> retryStatusCodeRanger = new HashMap<>();
//        retryStatusCodeRanger.put(500, true);
//        ParameterizedHttpSink sink = new ParameterizedHttpSink(client, retryStatusCodeRanger, stencilClient);
//        sink.pushMessage(Collections.singletonList(esbMessage));
//    }
//}
