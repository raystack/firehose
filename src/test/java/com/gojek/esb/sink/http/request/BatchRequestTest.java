package com.gojek.esb.sink.http.request;

import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.header.BasicHeader;
import com.gojek.esb.sink.http.request.uri.BasicUri;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BatchRequestTest {

    @Mock
    private BasicUri uri;

    @Mock
    private BasicHeader header;

    @Mock
    private JsonBody body;

    private List<EsbMessage> esbMessages;

    @Before
    public void setup() throws URISyntaxException {
        EsbMessage esbMessage = new EsbMessage(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        esbMessages = Collections.singletonList(esbMessage);
        when(uri.build()).thenReturn(new URI("http://dummy.com"));
    }

    @Test
    public void shouldPutJsonWrappedMessageInOneRequestForPUT() throws DeserializerException, URISyntaxException {

        HttpRequestMethod method = HttpRequestMethod.PUT;
        BatchRequest batchRequest = new BatchRequest(uri, header, body, method);
        List<HttpEntityEnclosingRequestBase> requests = batchRequest.build(esbMessages);

        assertEquals(1, requests.size());
    }

    @Test
    public void shouldPutJsonWrappedMessageInOneRequestForPOST() throws DeserializerException, URISyntaxException {

        HttpRequestMethod method = HttpRequestMethod.POST;
        BatchRequest batchRequest = new BatchRequest(uri, header, body, method);
        List<HttpEntityEnclosingRequestBase> requests = batchRequest.build(esbMessages);

        assertEquals(1, requests.size());
    }

    @Test
    public void shouldDelegatePUTRequestContentBuilding() throws DeserializerException, URISyntaxException {

        HttpRequestMethod method = HttpRequestMethod.PUT;
        BatchRequest batchRequest = new BatchRequest(uri, header, body, method);
        batchRequest.build(esbMessages);

        verify(uri, times(2)).build();
        verify(header, times(2)).build();
        verify(body, times(2)).serialize(esbMessages);
    }

    @Test
    public void shouldDelegatePOSTRequestContentBuilding() throws DeserializerException, URISyntaxException {

        HttpRequestMethod method = HttpRequestMethod.POST;
        BatchRequest batchRequest = new BatchRequest(uri, header, body, method);
        batchRequest.build(esbMessages);

        verify(uri, times(2)).build();
        verify(header, times(2)).build();
        verify(body, times(2)).serialize(esbMessages);
    }
}
