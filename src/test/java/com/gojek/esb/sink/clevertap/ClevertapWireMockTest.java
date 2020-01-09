//package com.gojek.esb.sink.clevertap;
//
//import com.github.tomakehurst.wiremock.junit.WireMockRule;
//import com.gojek.esb.config.ClevertapSinkConfig;
//import com.gojek.esb.metrics.StatsDReporter;
//import com.gojek.esb.util.Clock;
//import org.aeonbits.owner.ConfigFactory;
//import org.apache.http.HttpResponse;
//import org.apache.http.impl.client.CloseableHttpClient;
//import org.apache.http.impl.client.HttpClients;
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//import org.mockito.Mock;
//
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Properties;
//
//import static com.github.tomakehurst.wiremock.client.WireMock.*;
//import static org.mockito.MockitoAnnotations.initMocks;
//
//
//public class ClevertapWireMockTest {
//    @Mock
//    private StatsDReporter statsDReporter;
//
//    private ClevertapEvent event;
//
//    @Rule
//    public WireMockRule wireMockRule = new WireMockRule(9123);
//    private ClevertapSinkConfig config;
//    private CloseableHttpClient httpClient;
//
//    @Before
//    public void setup() {
//        initMocks(this);
//        Map<String, Object> eventData = new HashMap<>();
//        eventData.put("field1", 10);
//        eventData.put("field3", 20.111);
//        eventData.put("field2", "something");
//        event = new ClevertapEvent("EventA", "event", 111, "userid", eventData);
//
//
//        Properties properties = new Properties();
//        properties.put("SERVICE_URL", "http://localhost:9123/1/upload");
//        properties.put("HTTP_HEADERS", "X-CleverTap-Account-Id:x-account-id,X-CleverTap-Passcode:x-passcode");
//        config = ConfigFactory.create(ClevertapSinkConfig.class, properties);
//        httpClient = HttpClients.createDefault();
//    }
//
//    @Test
//    public void shouldSendEventToClevertap() throws IOException {
//        Clevertap clevertap = new Clevertap(config, httpClient, new Clock(), statsDReporter);
//        HttpResponse response = clevertap.sendEvents(Arrays.asList(event));
//        verify(postRequestedFor(urlMatching("/1/upload"))
//                .withRequestBody(equalTo("{d:["
//                        + "{\"evtName\":\"EventA\",\"type\":\"event\",\"ts\":111,\"identity\":\"userid\","
//                        + "\"evtData\":{"
//                        + "\"field1\":10,\"field3\":20.111,\"field2\":\"something\""
//                        + "}}]}"))
//                .withHeader("Content-Type", matching("application/json; charset=UTF-8"))
//                .withHeader("X-CleverTap-Account-Id", matching("x-account-id"))
//                .withHeader("X-CleverTap-Passcode", matching("x-passcode")));
//    }
//}
