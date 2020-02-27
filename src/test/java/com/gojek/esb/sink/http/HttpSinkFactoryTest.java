package com.gojek.esb.sink.http;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.util.Clock;
import org.gradle.internal.impldep.org.junit.Before;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.verify.VerificationTimes;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

@RunWith(MockitoJUnitRunner.class)
public class HttpSinkFactoryTest {
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private StencilClient stencilClient;

    private List<EsbMessage> esbMessages = Collections.singletonList(
            new EsbMessage(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100));

    private static ClientAndServer mockServer;

    @Before
    public void setup() throws URISyntaxException {
        initMocks(this);
    }

    @BeforeClass
    public static void startServer() {
        mockServer = startClientAndServer(1080);
    }

    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }

    @org.junit.Before
    public void startMockServer() {
        mockServer.reset();
        mockServer.when(request().withPath("/oauth2/token"))
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        mockServer.when(request().withPath("/api"))
                .respond(response().withStatusCode(200).withBody("OK"));
    }

    @Test(expected = Test.None.class)
    public void shouldNotEmbedAccessTokenIfGoAuthDisabled() throws IOException, DeserializerException {
        Map<String, String> configuration = new HashMap<>();
        configuration.put("HTTP_SINK_OAUTH2_ENABLED", "false");
        configuration.put("HTTP_SINK_OAUTH2_ACCESS_TOKEN_URL", "http://127.0.0.1:1080/oauth2/token");
        configuration.put("SERVICE_URL", "http://127.0.0.1:1080/api");
        AbstractSink sink = new HttpSinkFactory().create(configuration, statsDReporter, stencilClient);

        when(statsDReporter.getClock()).thenReturn(new Clock());
        sink.pushMessage(esbMessages);

        mockServer.verify(request().withPath("/oauth2/token"), VerificationTimes.exactly(0));
    }

    @Test(expected = Test.None.class)
    public void shouldEmbedAccessTokenIfGoAuthEnabled() throws IOException, DeserializerException {
        Map<String, String> configuration = new HashMap<>();
        configuration.put("HTTP_SINK_OAUTH2_ENABLED", "true");
        configuration.put("HTTP_SINK_OAUTH2_ACCESS_TOKEN_URL", "http://127.0.0.1:1080/oauth2/token");
        configuration.put("SERVICE_URL", "http://127.0.0.1:1080/api");
        AbstractSink sink = new HttpSinkFactory().create(configuration, statsDReporter, stencilClient);

        when(statsDReporter.getClock()).thenReturn(new Clock());
        sink.pushMessage(esbMessages);

        mockServer.verify(request().withPath("/oauth2/token"), VerificationTimes.exactly(1));
    }
}
