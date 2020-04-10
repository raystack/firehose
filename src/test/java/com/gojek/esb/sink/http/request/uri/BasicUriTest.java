package com.gojek.esb.sink.http.request.uri;

import com.gojek.esb.consumer.EsbMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class BasicUriTest {

    @Mock
    private EsbMessage esbMessage;
    @Mock
    private UriParser uriParser;

    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void shouldReturnURIInstanceBasedOnBaseUrl() {
        BasicUri basicUri = new BasicUri("http://dummy.com");
        try {
            assertEquals(new URI("http://dummy.com"), basicUri.build());
        } catch (URISyntaxException e) {
            new RuntimeException(e);
        }
    }

    @Test
    public void shouldReturnParsedURIInstanceBasedOnBaseUrl() throws URISyntaxException {
        String serviceUrl = "http://dummy.com/%s,6";
        when(uriParser.parse(esbMessage, serviceUrl)).thenReturn("http://dummy.com/protoField");

        BasicUri basicUri = new BasicUri(serviceUrl);

        assertEquals(new URI("http://dummy.com/protoField"), basicUri.build(esbMessage, uriParser));
    }
}
