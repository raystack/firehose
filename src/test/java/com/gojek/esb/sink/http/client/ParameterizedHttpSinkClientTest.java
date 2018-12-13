package com.gojek.esb.sink.http.client;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.gojek.esb.config.enums.HttpSinkParameterPlacementType;
import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.consumer.TestKey;
import com.gojek.esb.consumer.TestMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.sink.http.client.deserializer.Deserializer;
import com.gojek.esb.util.Clock;
import org.apache.http.HttpResponse;
import org.apache.http.impl.client.HttpClients;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ParameterizedHttpSinkClientTest {

    private ParameterizedHttpSinkClient client;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule();

    private String bookingServiceUrl = "http://localhost:8080/foobar";
    private List<EsbMessage> messages;
    private EsbMessage esbMessage;

    private TestMessage message;
    private TestKey key;

    private String serializedKey;
    private String serializedValue;

    @Mock
    private StatsDReporter statsDReporter;

    private Deserializer deserializer;

    @Mock
    private ProtoToFieldMapper protoToFieldMapper;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        message = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();

        EsbMessage msg1 = new EsbMessage(key.toByteArray(), message.toByteArray(), "topic1", 0, 100);
        EsbMessage msg2 = new EsbMessage(key.toByteArray(), message.toByteArray(), "topic2", 0, 100);

        messages = new ArrayList<EsbMessage>() {{
            add(msg1);
            add(msg2);
        }};

        esbMessage = msg1;
        serializedKey = msg1.getSerializedKey().replaceAll("=", "\\\\u003d");
        serializedValue = msg1.getSerializedMessage().replaceAll("=", "\\\\u003d");
        deserializer = Deserializer.build();
        client = new ParameterizedHttpSinkClient(bookingServiceUrl, new Header("headerkey:headervalue,headerkey2:headervalue2"), deserializer,
                protoToFieldMapper, HttpSinkParameterSourceType.DISABLED, HttpSinkParameterPlacementType.HEADER,
                HttpClients.createDefault(), new Clock(), statsDReporter);
    }

    @Test
    public void shouldSendSingleMessageToGivenUrl() throws DeserializerException, URISyntaxException {
        HttpResponse actual = client.execute(esbMessage);

        assertNotNull(actual);
        verify(putRequestedFor(urlMatching("/foobar"))
                .withRequestBody(equalTo("["
                        + "{\"topic\":\"topic1\",\"log_key\":\"" + serializedKey + "\",\"log_message\":\"" + serializedValue + "\"}"
                        + "]"))
                .withHeader("Content-Type", matching("application/json; charset=UTF-8")));
    }

    @Test
    public void shouldReceiveFieldsFromProtoToFieldMapper() throws DeserializerException, URISyntaxException {
        client.execute(esbMessage);
        when(protoToFieldMapper.getFields(any(byte[].class))).thenReturn(new HashMap<>());
        Mockito.verify(protoToFieldMapper, Mockito.times(1)).getFields(any(byte[].class));
    }

    @Test
    public void shouldAddParamsToUrlIfPlacementIsQuery() throws URISyntaxException, DeserializerException {
        ParameterizedHttpSinkClient clientQueryPlacement = new ParameterizedHttpSinkClient(
                bookingServiceUrl, new Header("headerkey:headervalue,headerkey2:headervalue2"),
                Deserializer.build(), protoToFieldMapper, HttpSinkParameterSourceType.KEY, HttpSinkParameterPlacementType.QUERY,
                HttpClients.createDefault(), new Clock(), statsDReporter);
        HashMap<String, Object> queryParamMap = new HashMap<>();
        queryParamMap.put("cash_amount", 1234);
        queryParamMap.put("order_number", "RB-34343");
        when(protoToFieldMapper.getFields(any(byte[].class))).thenReturn(queryParamMap);
        HttpResponse actual = clientQueryPlacement.execute(esbMessage);

        assertNotNull(actual);
        verify(putRequestedFor(urlPathMatching("/foobar"))
                .withQueryParam("cash_amount", equalTo("1234"))
                .withQueryParam("order_number", equalTo("RB-34343"))
                .withRequestBody(equalTo("["
                        + "{\"topic\":\"topic1\",\"log_key\":\"" + serializedKey + "\",\"log_message\":\"" + serializedValue + "\"}"
                        + "]"))
                .withHeader("Content-Type", matching("application/json; charset=UTF-8")));
    }

    @Test
    public void shouldAddCustomHeadersIfPlacementIsHeader() throws URISyntaxException, DeserializerException {
        ParameterizedHttpSinkClient clientHeaderPlacement = new ParameterizedHttpSinkClient(
                bookingServiceUrl, new Header("headerkey:headervalue,headerkey2:headervalue2"),
                Deserializer.build(), protoToFieldMapper, HttpSinkParameterSourceType.KEY, HttpSinkParameterPlacementType.HEADER,
                HttpClients.createDefault(), new Clock(), statsDReporter);

        HashMap<String, Object> paramMap = new HashMap<>();
        paramMap.put("cash_amount", 1234);
        paramMap.put("order_number", "RB-34343");
        when(protoToFieldMapper.getFields(any(byte[].class))).thenReturn(paramMap);
        HttpResponse actual = clientHeaderPlacement.execute(esbMessage);

        assertNotNull(actual);
        verify(putRequestedFor(urlPathMatching("/foobar"))
                .withRequestBody(equalTo("["
                        + "{\"topic\":\"topic1\",\"log_key\":\"" + serializedKey + "\",\"log_message\":\"" + serializedValue + "\"}"
                        + "]"))
                .withHeader("Content-Type", matching("application/json; charset=UTF-8"))
                .withHeader("X-CashAmount", equalTo("1234"))
                .withHeader("X-OrderNumber", equalTo("RB-34343")));
    }

    @Test(expected = URISyntaxException.class)
    public void shouldThrowInvalidURIExceptionForInvalidURI() throws IOException, DeserializerException, URISyntaxException {
        ParameterizedHttpSinkClient clientInvalidUrl = new ParameterizedHttpSinkClient(
                "http://localhos t/foobar", new Header("headerkey:headervalue,headerkey2:headervalue2"),
                Deserializer.build(), protoToFieldMapper, HttpSinkParameterSourceType.KEY, HttpSinkParameterPlacementType.QUERY,
                HttpClients.createDefault(), new Clock(), statsDReporter);

        clientInvalidUrl.execute(esbMessage);
        when(protoToFieldMapper.getFields(any(byte[].class))).thenReturn(new HashMap<>());
        Mockito.verify(protoToFieldMapper, Mockito.times(1)).getFields(any(byte[].class));
    }

    @Test
    public void shouldCallProtoToFieldMapperOnBasisOfParameterSource() throws URISyntaxException, IOException, DeserializerException {
        ParameterizedHttpSinkClient clientParameterSource = new ParameterizedHttpSinkClient(
                "http://localhost/foobar", new Header("headerkey:headervalue,headerkey2:headervalue2"),
                Deserializer.build(), protoToFieldMapper, HttpSinkParameterSourceType.KEY, HttpSinkParameterPlacementType.QUERY,
                HttpClients.createDefault(), new Clock(), statsDReporter);

        clientParameterSource.execute(esbMessage);
        when(protoToFieldMapper.getFields(any(byte[].class))).thenReturn(new HashMap<>());
        ArgumentCaptor<byte[]> argument = ArgumentCaptor.forClass(byte[].class);
        Mockito.verify(protoToFieldMapper).getFields(argument.capture());
        assertEquals(esbMessage.getLogKey(), argument.getValue());
    }
}
