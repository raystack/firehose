package com.gojek.esb.sink.http.request;

import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.header.BasicHeader;
import com.gojek.esb.sink.http.request.uri.BasicUri;
import com.gojek.esb.sink.http.request.uri.UriParser;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DynamicUrlRequestTest {

    @Mock
    private JsonBody jsonBody;
    @Mock
    private UriParser uriParser;
    @Mock
    private BasicUri basicUri;
    @Mock
    private BasicHeader basicHeader;

    @Test
    public void shouldCreatePUTRequestsPerEsbMessage() {
        EsbMessage esbMessage = new EsbMessage(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        List<EsbMessage> esbMessages = new ArrayList<EsbMessage>();
        esbMessages.add(esbMessage);
        esbMessages.add(esbMessage);

        try {
            List<String> bodyContent = new ArrayList<String>();
            bodyContent.add("{}");
            bodyContent.add("{}");
            when(jsonBody.serialize(esbMessages)).thenReturn(bodyContent);
        } catch (DeserializerException e) {
            throw new RuntimeException(e);
        }

        DynamicUrlRequest dynamicUrlRequest = new DynamicUrlRequest(basicUri, basicHeader, jsonBody, HttpRequestMethod.PUT, uriParser);

        try {
            dynamicUrlRequest.build(esbMessages);
            verify(basicUri, times(2)).build(esbMessage, uriParser);
            verify(basicHeader, times(4)).build();
            verify(jsonBody, times(1)).serialize(esbMessages);
        } catch (URISyntaxException | DeserializerException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldCreatePOSTRequestsPerEsbMessage() {
        EsbMessage esbMessage = new EsbMessage(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        List<EsbMessage> esbMessages = new ArrayList<EsbMessage>();
        esbMessages.add(esbMessage);
        esbMessages.add(esbMessage);

        try {
            List<String> bodyContent = new ArrayList<String>();
            bodyContent.add("{}");
            bodyContent.add("{}");
            when(jsonBody.serialize(esbMessages)).thenReturn(bodyContent);
        } catch (DeserializerException e) {
            throw new RuntimeException(e);
        }

        DynamicUrlRequest dynamicUrlRequest = new DynamicUrlRequest(basicUri, basicHeader, jsonBody, HttpRequestMethod.POST, uriParser);
        try {
            dynamicUrlRequest.build(esbMessages);
            verify(basicUri, times(2)).build(esbMessage, uriParser);
            verify(basicHeader, times(4)).build();
            verify(jsonBody, times(1)).serialize(esbMessages);
        } catch (URISyntaxException | DeserializerException e) {
            throw new RuntimeException(e);
        }
    }
}
