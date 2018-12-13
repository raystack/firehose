package com.gojek.esb.sink.clevertap;

import com.gojek.esb.booking.BookingLogKey;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.config.ClevertapSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.proto.ProtoMessage;
import com.gojek.esb.types.ServiceTypeProto;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.mockito.Mockito.verify;

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

    @Mock
    private Clevertap clevertap;
    private HashMap<String, Object> expectedEventData;
    private Properties properties;

    @Before
    public void setup() {

        properties = new Properties();
        properties.put("CLEVERTAP_SINK_EVENT_TIMESTAMP_INDEX", Integer.toString(BookingLogMessage.EVENT_TIMESTAMP_FIELD_NUMBER));
        properties.put("CLEVERTAP_SINK_USER_ID_INDEX", Integer.toString(BookingLogMessage.CUSTOMER_ID_FIELD_NUMBER));
        properties.put("CLEVERTAP_SINK_EVENT_NAME", clevertapEventName);
        properties.put("CLEVERTAP_SINK_EVENT_TYPE", clevertapEventType);
        properties.put("CLEVERTAP_SINK_EVENT_TYPE", clevertapEventType);

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
    }

    @Test
    public void shouldSendEventsWithSimpleFieldsToClevertap() throws IOException {
        properties.put("PROTO_TO_COLUMN_MAPPING", "{2:\"orderNumber\", 8:\"driverId\"}");
        expectedEventData.put("driverId", bookingLogMessage.getDriverId());
        expectedEventData.put("orderNumber", bookingLogMessage.getOrderNumber());
        config = ConfigFactory.create(ClevertapSinkConfig.class, properties);

        List<ClevertapEvent> expectedEvents = Arrays.asList(
                new ClevertapEvent(clevertapEventName, clevertapEventType, TIMESTAMP_IN_EPOCH_SECONDS, expectedCustomerId, expectedEventData));

        ClevertapSink clevertapSink = new ClevertapSink(config, clevertap, protoMessage);

        clevertapSink.pushMessage(Arrays.asList(esbMessage));

        verify(clevertap).sendEvents(expectedEvents);
    }

    @Test
    public void shouldSendEnumFieldsToClevertap() throws IOException {
        properties.put("PROTO_TO_COLUMN_MAPPING", "{1:\"serviceType\"}");
        expectedEventData.put("serviceType", bookingLogMessage.getServiceType().toString());
        config = ConfigFactory.create(ClevertapSinkConfig.class, properties);

        List<ClevertapEvent> expectedEvents = Arrays.asList(
                new ClevertapEvent(clevertapEventName, clevertapEventType, TIMESTAMP_IN_EPOCH_SECONDS, expectedCustomerId, expectedEventData));

        ClevertapSink clevertapSink = new ClevertapSink(config, clevertap, protoMessage);

        clevertapSink.pushMessage(Arrays.asList(esbMessage));

        verify(clevertap).sendEvents(expectedEvents);
    }

    @Test
    public void shouldSendDateFieldsToClevertap() throws IOException {
        properties.put("PROTO_TO_COLUMN_MAPPING", "{44:\"PickupTime\"}");
        String expectedPickupTime = String.format("$D_%d", bookingLogMessage.getPickupTime().getSeconds());
        expectedEventData.put("PickupTime", expectedPickupTime);
        config = ConfigFactory.create(ClevertapSinkConfig.class, properties);

        List<ClevertapEvent> expectedEvents = Arrays.asList(
                new ClevertapEvent(clevertapEventName, clevertapEventType, TIMESTAMP_IN_EPOCH_SECONDS, expectedCustomerId, expectedEventData));

        ClevertapSink clevertapSink = new ClevertapSink(config, clevertap, protoMessage);

        clevertapSink.pushMessage(Arrays.asList(esbMessage));

        verify(clevertap).sendEvents(expectedEvents);
    }

    @Test
    public void shouldSendDurationFieldsToClevertap() throws IOException {
        properties.put("PROTO_TO_COLUMN_MAPPING", "{25:\"DriverDropoffEta\"}");
        Long driverDropoffEta = bookingLogMessage.getDriverEtaDropoff().getSeconds();
        expectedEventData.put("DriverDropoffEta", driverDropoffEta);
        config = ConfigFactory.create(ClevertapSinkConfig.class, properties);

        List<ClevertapEvent> expectedEvents = Arrays.asList(
                new ClevertapEvent(clevertapEventName, clevertapEventType, TIMESTAMP_IN_EPOCH_SECONDS, expectedCustomerId, expectedEventData));

        ClevertapSink clevertapSink = new ClevertapSink(config, clevertap, protoMessage);

        clevertapSink.pushMessage(Arrays.asList(esbMessage));

        verify(clevertap).sendEvents(expectedEvents);
    }
}
