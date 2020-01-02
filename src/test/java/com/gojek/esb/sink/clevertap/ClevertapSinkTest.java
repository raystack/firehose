package com.gojek.esb.sink.clevertap;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.gojek.esb.booking.BookingLogKey;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.config.ClevertapSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.proto.ProtoMessage;
import com.gojek.esb.types.ServiceTypeProto;
import com.google.gson.GsonBuilder;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseFactory;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.message.BasicStatusLine;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ClevertapSinkTest {

    private static final long TIMESTAMP_IN_EPOCH_SECONDS = 1498600;
    private final String orderNumber = "lol";
    private final String driverId = "3";
    private final float amountPaidByCash = 3.14f;
    private EsbMessage esbMessage;
    private ClevertapSinkConfig config;
    private String clevertapEventName = "cleEventName";
    private String clevertapEventType = "event";
    private ProtoMessage protoMessage = new ProtoMessage(BookingLogMessage.class.getName());
    private String expectedCustomerId = "1112233";
    private BookingLogMessage bookingLogMessage;

    private HashMap<String, Object> expectedEventData;
    private Properties properties;
    private static final String CLEVERTAP = "clevertap";
    @Mock
    private HttpClient httpClient;
    @Mock
    private Instrumentation instrumentation;
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(9123);

    @Before
    public void setup() throws IOException {

        properties = new Properties();
        properties.put("CLEVERTAP_SINK_EVENT_TIMESTAMP_INDEX", Integer.toString(BookingLogMessage.EVENT_TIMESTAMP_FIELD_NUMBER));
        properties.put("CLEVERTAP_SINK_USER_ID_INDEX", Integer.toString(BookingLogMessage.CUSTOMER_ID_FIELD_NUMBER));
        properties.put("CLEVERTAP_SINK_EVENT_NAME", clevertapEventName);
        properties.put("CLEVERTAP_SINK_EVENT_TYPE", clevertapEventType);
        properties.put("CLEVERTAP_SINK_EVENT_TYPE", clevertapEventType);
        properties.put("SERVICE_URL", "dummyUrl");

        bookingLogMessage = BookingLogMessage.newBuilder().setDriverId(driverId).setOrderNumber(orderNumber)
                .setCustomerId(expectedCustomerId)
                .setServiceType(ServiceTypeProto.ServiceType.Enum.GO_CAR)
                .setPickupTime(
                        Timestamp.newBuilder().setSeconds(12345).build()
                )
                .setDriverEtaDropoff(
                        Duration.newBuilder().setSeconds(54321).build()
                )
                .setAmountPaidByCash(amountPaidByCash).setEventTimestamp(
                        Timestamp.newBuilder().setSeconds(TIMESTAMP_IN_EPOCH_SECONDS).build()
                ).build();
        BookingLogKey bookingLogKey = BookingLogKey.newBuilder().setOrderNumber(orderNumber).build();

        esbMessage = new EsbMessage(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray(), "", 0, 0);

        expectedEventData = new HashMap<>();


        HttpResponseFactory factory = new DefaultHttpResponseFactory();
        HttpResponse response = factory.newHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, null), null);


        when(httpClient.execute(any())).thenReturn(response);
    }

    @Test
    public void shouldPrepareEventsWithSimpleFields() {
        properties.put("PROTO_TO_COLUMN_MAPPING", "{2:\"orderNumber\", 8:\"driverId\"}");
        expectedEventData.put("driverId", bookingLogMessage.getDriverId());
        expectedEventData.put("orderNumber", bookingLogMessage.getOrderNumber());
        config = ConfigFactory.create(ClevertapSinkConfig.class, properties);

        List<ClevertapEvent> expectedEvents = Arrays.asList(
                new ClevertapEvent(clevertapEventName, clevertapEventType, TIMESTAMP_IN_EPOCH_SECONDS, expectedCustomerId, expectedEventData));

        String expectedEventString = new GsonBuilder().create().toJson(expectedEvents);

        ClevertapSink clevertapSink = new ClevertapSink(instrumentation, CLEVERTAP, config, protoMessage, httpClient);

        clevertapSink.prepare(Arrays.asList(esbMessage));
        verify(instrumentation).logDebug("{d:%s}", expectedEventString);
    }

    @Test
    public void shouldPrepareEnumFieldsToClevertap() {
        properties.put("PROTO_TO_COLUMN_MAPPING", "{1:\"serviceType\"}");
        expectedEventData.put("serviceType", bookingLogMessage.getServiceType().toString());
        config = ConfigFactory.create(ClevertapSinkConfig.class, properties);

        List<ClevertapEvent> expectedEvents = Arrays.asList(
                new ClevertapEvent(clevertapEventName, clevertapEventType, TIMESTAMP_IN_EPOCH_SECONDS, expectedCustomerId, expectedEventData));
        String expectedEventString = new GsonBuilder().create().toJson(expectedEvents);

        ClevertapSink clevertapSink = new ClevertapSink(instrumentation, CLEVERTAP, config, protoMessage, httpClient);

        clevertapSink.prepare(Arrays.asList(esbMessage));

        verify(instrumentation).logDebug("{d:%s}", expectedEventString);
    }

    @Test
    public void shouldPrepareDateFields() {
        properties.put("PROTO_TO_COLUMN_MAPPING", "{44:\"PickupTime\"}");
        String expectedPickupTime = String.format("$D_%d", bookingLogMessage.getPickupTime().getSeconds());
        expectedEventData.put("PickupTime", expectedPickupTime);
        config = ConfigFactory.create(ClevertapSinkConfig.class, properties);

        List<ClevertapEvent> expectedEvents = Arrays.asList(
                new ClevertapEvent(clevertapEventName, clevertapEventType, TIMESTAMP_IN_EPOCH_SECONDS, expectedCustomerId, expectedEventData));
        String expectedEventString = new GsonBuilder().create().toJson(expectedEvents);


        ClevertapSink clevertapSink = new ClevertapSink(instrumentation, CLEVERTAP, config, protoMessage, httpClient);

        clevertapSink.prepare(Arrays.asList(esbMessage));

        verify(instrumentation).logDebug("{d:%s}", expectedEventString);
    }

    @Test
    public void shouldPrepareDurationFields() {
        properties.put("PROTO_TO_COLUMN_MAPPING", "{25:\"DriverDropoffEta\"}");
        Long driverDropoffEta = bookingLogMessage.getDriverEtaDropoff().getSeconds();
        expectedEventData.put("DriverDropoffEta", driverDropoffEta);
        config = ConfigFactory.create(ClevertapSinkConfig.class, properties);

        List<ClevertapEvent> expectedEvents = Arrays.asList(
                new ClevertapEvent(clevertapEventName, clevertapEventType, TIMESTAMP_IN_EPOCH_SECONDS, expectedCustomerId, expectedEventData));
        String expectedEventString = new GsonBuilder().create().toJson(expectedEvents);

        ClevertapSink clevertapSink = new ClevertapSink(instrumentation, CLEVERTAP, config, protoMessage, httpClient);

        clevertapSink.prepare(Arrays.asList(esbMessage));

        verify(instrumentation).logDebug("{d:%s}", expectedEventString);
    }

    @Test
    public void shouldExecuteRequestSuccessfully() throws Exception {
        properties.put("PROTO_TO_COLUMN_MAPPING", "{25:\"DriverDropoffEta\"}");
        config = ConfigFactory.create(ClevertapSinkConfig.class, properties);
        ClevertapSink clevertapSink = new ClevertapSink(instrumentation, CLEVERTAP, config, protoMessage, httpClient);

        List<EsbMessage> execute = clevertapSink.execute();

        verify(instrumentation, times(1)).logInfo("Response Status: {}", 200);
        verify(instrumentation, times(1)).captureHttpStatusCount(any(), any(HttpResponse.class));
        Assert.assertEquals(0, execute.size());
    }

    @Test(expected = IOException.class)
    public void shouldThrowExceptionAndCaptureFatalError() throws Exception {
        properties.put("PROTO_TO_COLUMN_MAPPING", "{25:\"DriverDropoffEta\"}");
        config = ConfigFactory.create(ClevertapSinkConfig.class, properties);
        ClevertapSink clevertapSink = new ClevertapSink(instrumentation, CLEVERTAP, config, protoMessage, httpClient);

        IOException ioException = new IOException();
        when(httpClient.execute(any())).thenThrow(ioException);

        clevertapSink.execute();

        verify(instrumentation, times(1)).captureFatalError(ioException, "Error while calling http sink service url");
        verify(instrumentation, times(1)).captureHttpStatusCount(any(), any(HttpResponse.class));
    }

}
