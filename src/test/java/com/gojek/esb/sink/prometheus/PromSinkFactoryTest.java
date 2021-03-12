package com.gojek.esb.sink.prometheus;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.consumer.Message;
import com.gojek.esb.consumer.TestFeedbackLogMessage;
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
public class PromSinkFactoryTest {
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private StencilClient stencilClient;

    private static ClientAndServer mockServer;

    @Before
    public void setup() {
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

    @Test
    public void shouldNotEmbedAccessTokenIfGoAuthDisabled() throws DeserializerException {
        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder().setTipAmount(100).build();
        byte[] message = feedbackLogMessage.toByteArray();
        List<Message> messages = Collections.singletonList(
                new Message(new byte[]{10, 20}, message, "sample-topic", 0, 100));

        Map<String, String> configuration = new HashMap<>();
        configuration.put("input.schema.proto.class", "com.gojek.esb.consumer.TestFeedbackLogMessage");
        configuration.put("sink.prom.metric.name.proto.index.mapping", "{\"7\": \"tip_amount\"}");
        configuration.put("sink.prom.oauth2.enable", "false");
        configuration.put("sink.prom.oauth2.access.token.url", "http://127.0.0.1:1080/oauth2/token");
        configuration.put("sink.prom.service.url", "http://127.0.0.1:1080/api");
        AbstractSink sink = new PromSinkFactory().create(configuration, statsDReporter, stencilClient);

        when(stencilClient.get("com.gojek.esb.consumer.TestFeedbackLogMessage")).thenReturn(TestFeedbackLogMessage.getDescriptor());
        when(statsDReporter.getClock()).thenReturn(new Clock());
        sink.pushMessage(messages);

        mockServer.verify(request().withPath("/oauth2/token"), VerificationTimes.exactly(0));
    }

    @Test
    public void shouldEmbedAccessTokenIfGoAuthEnabled() throws DeserializerException {
        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder().setTipAmount(100).build();
        byte[] message = feedbackLogMessage.toByteArray();
        List<Message> messages = Collections.singletonList(
                new Message(new byte[]{10, 20}, message, "sample-topic", 0, 100));
        Map<String, String> configuration = new HashMap<>();
        configuration.put("input.schema.proto.class", "com.gojek.esb.consumer.TestFeedbackLogMessage");
        configuration.put("sink.prom.metric.name.proto.index.mapping", "{\"7\": \"tip_amount\"}");
        configuration.put("sink.prom.oauth2.enable", "true");
        configuration.put("sink.prom.oauth2.access.token.url", "http://127.0.0.1:1080/oauth2/token");
        configuration.put("sink.prom.service.url", "http://127.0.0.1:1080/api");
        AbstractSink sink = new PromSinkFactory().create(configuration, statsDReporter, stencilClient);

        when(stencilClient.get("com.gojek.esb.consumer.TestFeedbackLogMessage")).thenReturn(TestFeedbackLogMessage.getDescriptor());
        when(statsDReporter.getClock()).thenReturn(new Clock());
        sink.pushMessage(messages);

        mockServer.verify(request().withPath("/oauth2/token"), VerificationTimes.exactly(1));
    }
}
