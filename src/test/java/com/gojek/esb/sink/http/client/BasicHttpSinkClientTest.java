//package com.gojek.esb.sink.http.client;
//
//import com.github.tomakehurst.wiremock.junit.WireMockRule;
//import com.gojek.esb.consumer.EsbMessage;
//import com.gojek.esb.consumer.TestKey;
//import com.gojek.esb.consumer.TestMessage;
//import com.gojek.esb.exception.DeserializerException;
//import com.gojek.esb.metrics.StatsDReporter;
//import com.gojek.esb.sink.http.client.deserializer.Deserializer;
//import com.gojek.esb.util.Clock;
//import org.apache.http.HttpResponse;
//import org.apache.http.impl.client.HttpClients;
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//import org.mockito.Mock;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import static com.github.tomakehurst.wiremock.client.WireMock.*;
//import static org.junit.Assert.assertNotNull;
//import static org.mockito.MockitoAnnotations.initMocks;
//
//public class BasicHttpSinkClientTest {
//
//    private BasicHttpSinkClient client;
//
//    @Rule
//    public WireMockRule wireMockRule = new WireMockRule();
//
//    private String bookingServiceUrl = "http://localhost:8080/foobar";
//    private List<EsbMessage> messages;
//    private EsbMessage esbMessage;
//
//    private TestMessage message;
//    private TestKey key;
//
//    private String serializedKey;
//    private String serializedValue;
//
//    @Mock
//    private StatsDReporter statsDReporter;
//
//    private Deserializer deserializer;
//
//    @Before
//    public void setUp() throws Exception {
//        initMocks(this);
//        message = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
//        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
//
//        EsbMessage msg1 = new EsbMessage(key.toByteArray(), message.toByteArray(), "topic1", 0, 100);
//        EsbMessage msg2 = new EsbMessage(key.toByteArray(), message.toByteArray(), "topic2", 0, 100);
//
//        messages = new ArrayList<EsbMessage>() {{
//            add(msg1);
//            add(msg2);
//        }};
//
//        esbMessage = msg1;
//        serializedKey = msg1.getSerializedKey().replaceAll("=", "\\\\u003d");
//        serializedValue = msg1.getSerializedMessage().replaceAll("=", "\\\\u003d");
//        deserializer = Deserializer.build();
//        client = new BasicHttpSinkClient(bookingServiceUrl, new Header("headerkey:headervalue,headerkey2:headervalue2"), deserializer,
//                HttpClients.createDefault(), new Clock(), statsDReporter);
//    }
//
//    @Test
//    public void putsMessagesToGivenURL() throws DeserializerException {
//        HttpResponse actual = client.executeBatch(messages);
//
//        assertNotNull(actual);
//        verify(putRequestedFor(urlMatching("/foobar"))
//                .withRequestBody(equalTo("["
//                        + "{\"topic\":\"topic1\",\"log_key\":\"" + serializedKey + "\",\"log_message\":\"" + serializedValue + "\"}, "
//                        + "{\"topic\":\"topic2\",\"log_key\":\"" + serializedKey + "\",\"log_message\":\"" + serializedValue + "\"}"
//                        + "]"))
//                .withHeader("Content-Type", matching("application/json; charset=UTF-8")));
//    }
//
//    @Test
//    public void putsJsonMessagesToGivenURL() throws DeserializerException {
//        HttpResponse actual = client.executeBatch(messages);
//
//        assertNotNull(actual);
//        verify(putRequestedFor(urlMatching("/foobar"))
//                .withRequestBody(equalTo("["
//                        + "{\"topic\":\"topic1\",\"log_key\":\"" + serializedKey + "\",\"log_message\":\"" + serializedValue + "\"}, "
//                        + "{\"topic\":\"topic2\",\"log_key\":\"" + serializedKey + "\",\"log_message\":\"" + serializedValue + "\"}"
//                        + "]"))
//                .withHeader("Content-Type", matching("application/json; charset=UTF-8")));
//    }
//
//    @Test
//    public void shouldAddHeadersFromHeaderConfig() throws DeserializerException {
//        HttpResponse actual = client.executeBatch(messages);
//
//        assertNotNull(actual);
//        verify(putRequestedFor(urlMatching("/foobar"))
//                .withHeader("headerkey", matching("headervalue"))
//                .withHeader("headerkey2", matching("headervalue2")));
//    }
//}
