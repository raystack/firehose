package com.gojek.esb.sink.http.request;

import com.gojek.esb.config.enums.HttpRequestMethod;
import com.gojek.esb.sink.http.request.uri.BasicUri;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpRequestMethodFactoryTest {

    @Mock
    private BasicUri uri;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        when(uri.build()).thenReturn(new URI("http://dummy.com"));

    }

    @Test
    public void shouldReturnHttpPutRequest() throws URISyntaxException {
        HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory.create(uri.build(), HttpRequestMethod.PUT);

        assertTrue(request instanceof HttpPut);
    }

    @Test
    public void shouldReturnHttpPostRequest() throws URISyntaxException {
        HttpEntityEnclosingRequestBase request = HttpRequestMethodFactory.create(uri.build(), HttpRequestMethod.POST);

        assertTrue(request instanceof HttpPost);
    }
}
