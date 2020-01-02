//package com.gojek.esb.sink;
//
//import com.gojek.de.stencil.client.StencilClient;
//import com.gojek.esb.consumer.EsbMessage;
//import com.gojek.esb.exception.DeserializerException;
//import com.gojek.esb.sink.http.HttpSink;
//import com.gojek.esb.sink.http.client.BasicHttpSinkClient;
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
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//
//import static org.junit.Assert.assertEquals;
//import static org.mockito.Mockito.when;
//
//@RunWith(MockitoJUnitRunner.class)
//public class HttpSinkTest {
//
//    @Mock
//    private EsbMessage esbMessage;
//
//    @Mock
//    private BasicHttpSinkClient client;
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
//    public void setUp() throws DeserializerException {
//        Mockito.when(client.executeBatch(Mockito.anyList())).thenReturn(httpResponse);
//        when(statusLine.getStatusCode()).thenReturn(200);
//        when(httpResponse.getStatusLine()).thenReturn(statusLine);
//    }
//
//    @Test
//    public void shouldWriteToSink() throws Exception {
//        HashMap<Integer, Boolean> retryStatusCodeRanger = new HashMap<>();
//        retryStatusCodeRanger.put(500, true);
//        HttpSink sink = new HttpSink(client, retryStatusCodeRanger, stencilClient);
//        sink.pushMessage(Collections.singletonList(esbMessage));
//
//        Mockito.verify(client, Mockito.times(1)).executeBatch(Mockito.anyList());
//    }
//
//    @Test
//    public void shouldWriteJsonToSink() throws Exception {
//        HashMap<Integer, Boolean> retryStatusCodeRanger = new HashMap<>();
//        retryStatusCodeRanger.put(500, true);
//        HttpSink sink = new HttpSink(client, retryStatusCodeRanger, stencilClient);
//        sink.pushMessage(Collections.singletonList(esbMessage));
//
//        Mockito.verify(client, Mockito.times(1)).executeBatch(Mockito.anyList());
//    }
//
//    @Test
//    public void shouldSendBackEmptyListOnSuccess() throws IOException, DeserializerException {
//        HashMap<Integer, Boolean> retryStatusCodeRanger = new HashMap<>();
//        retryStatusCodeRanger.put(500, true);
//        HttpSink sink = new HttpSink(client, retryStatusCodeRanger, stencilClient);
//        List<EsbMessage> failedMessages = sink.pushMessage(Collections.singletonList(esbMessage));
//
//        assertEquals(failedMessages.size(), 0);
//    }
//
//    @Test
//    public void shouldSendBackListOfFailedMessagesIfResponseIsInGivenRange() throws IOException, DeserializerException {
//        Mockito.when(client.executeBatch(Mockito.anyList())).thenReturn(httpResponse);
//        when(statusLine.getStatusCode()).thenReturn(500);
//        when(httpResponse.getStatusLine()).thenReturn(statusLine);
//        HashMap<Integer, Boolean> retryStatusCodeRanger = new HashMap<>();
//        retryStatusCodeRanger.put(500, true);
//        HttpSink sink = new HttpSink(client, retryStatusCodeRanger, stencilClient);
//        ArrayList<EsbMessage> esbMessages = new ArrayList<>();
//        esbMessages.add(esbMessage);
//        esbMessages.add(esbMessage);
//        esbMessages.add(esbMessage);
//        List<EsbMessage> failedMessages = sink.pushMessage(esbMessages);
//
//        assertEquals(failedMessages.size(), 3);
//    }
//
//    @Test
//    public void shouldSendBackListOfFailedMessagesIfResponseIsNull() throws IOException, DeserializerException {
//        Mockito.when(client.executeBatch(Mockito.anyList())).thenReturn(null);
//        HashMap<Integer, Boolean> retryStatusCodeRanger = new HashMap<>();
//        retryStatusCodeRanger.put(500, true);
//        HttpSink sink = new HttpSink(client, retryStatusCodeRanger, stencilClient);
//        List<EsbMessage> failedMessages = sink.pushMessage(Collections.singletonList(esbMessage));
//
//        assertEquals(failedMessages.size(), 1);
//    }
//}
