package com.gojek.esb.sink.http.request;

import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.sink.http.request.body.JsonBody;
import com.gojek.esb.sink.http.request.header.ParameterizedHeader;
import com.gojek.esb.sink.http.request.uri.ParameterizedUri;
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
public class ParameterizedRequestTest {
    @Mock
    private ParameterizedUri parameterizedUri;
    @Mock
    private ParameterizedHeader parameterizedHeader;
    @Mock
    private JsonBody jsonBody;
    @Mock
    private UriParser uriParser;

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

        HttpRequestMethod httpRequestMethod = HttpRequestMethod.PUT;
        ParameterizedRequest multipleRequest = new ParameterizedRequest(parameterizedUri, parameterizedHeader, jsonBody, httpRequestMethod, uriParser);

        try {
            multipleRequest.build(esbMessages);
            verify(parameterizedUri, times(2)).build(esbMessage, uriParser);
            verify(parameterizedHeader, times(4)).build(esbMessage);
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

        HttpRequestMethod httpRequestMethod = HttpRequestMethod.POST;
        ParameterizedRequest multipleRequest = new ParameterizedRequest(parameterizedUri, parameterizedHeader, jsonBody, httpRequestMethod, uriParser);

        try {
            multipleRequest.build(esbMessages);
            verify(parameterizedUri, times(2)).build(esbMessage, uriParser);
            verify(parameterizedHeader, times(4)).build(esbMessage);
            verify(jsonBody, times(1)).serialize(esbMessages);
        } catch (URISyntaxException | DeserializerException e) {
            throw new RuntimeException(e);
        }
    }

}
