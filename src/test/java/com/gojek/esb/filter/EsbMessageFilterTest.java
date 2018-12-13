package com.gojek.esb.filter;

import com.gojek.esb.booking.BookingLogKey;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.consumer.TestKey;
import com.gojek.esb.consumer.TestMessage;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class EsbMessageFilterTest {

    private KafkaConsumerConfig kafkaConsumerConfig;

    private Filter filter;

    private TestMessage message;
    private TestKey key;

    @Before
    public void setup() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_TYPE", "message");
        filterConfigs.put("FILTER_EXPRESSION", "testMessage.getOrderNumber() == 123");
        filterConfigs.put("FILTER_PROTO_SCHEMA", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        filter = new EsbMessageFilter(kafkaConsumerConfig);
        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        message = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
    }

    @Test
    public void shouldFilterEsbMessages() throws EsbFilterException {
        EsbMessage esbMessage = new EsbMessage(key.toByteArray(), this.message.toByteArray(), "topic1", 0, 100);
        List<EsbMessage> filteredMessages = filter.filter(Arrays.asList(esbMessage));
        assertEquals(filteredMessages.get(0), esbMessage);
    }

    @Test
    public void shouldNotFilterEsbMessagesForEmptyBooleanValues() throws EsbFilterException {
        BookingLogMessage bookingLogMessage = BookingLogMessage.newBuilder().setCustomerId("customerId").build();
        BookingLogKey bookingLogKey = BookingLogKey.newBuilder().build();
        EsbMessage esbMessage = new EsbMessage(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray(), "topic1", 0, 100);
        HashMap<String, String> bookingFilterConfigs = new HashMap<>();
        bookingFilterConfigs.put("FILTER_TYPE", "message");
        bookingFilterConfigs.put("FILTER_EXPRESSION", "bookingLogMessage.getCustomerDynamicSurgeEnabled() == false");
        bookingFilterConfigs.put("FILTER_PROTO_SCHEMA", BookingLogMessage.class.getName());
        KafkaConsumerConfig bookingConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, bookingFilterConfigs);
        EsbMessageFilter bookingFilter = new EsbMessageFilter(bookingConsumerConfig);
        List<EsbMessage> filteredMessages = bookingFilter.filter(Arrays.asList(esbMessage));
        assertEquals(filteredMessages.get(0), esbMessage);
    }

    @Test(expected = EsbFilterException.class)
    public void shouldThrowExceptionOnInvalidFilterExpression() throws EsbFilterException {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_TYPE", "message");
        filterConfigs.put("FILTER_EXPRESSION", "1+2");
        filterConfigs.put("FILTER_PROTO_SCHEMA", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        filter = new EsbMessageFilter(kafkaConsumerConfig);
        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        message = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();

        EsbMessage esbMessage = new EsbMessage(key.toByteArray(), this.message.toByteArray(), "topic1", 0, 100);
        filter.filter(Arrays.asList(esbMessage));
    }

    @Test
    public void shouldNotApplyFilterOnEmptyFilterType() throws EsbFilterException {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_TYPE", "");
        filterConfigs.put("FILTER_EXPRESSION", "testMessage.getOrderNumber() == 123");
        filterConfigs.put("FILTER_PROTO_SCHEMA", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        filter = new EsbMessageFilter(kafkaConsumerConfig);
        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        message = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();

        EsbMessage esbMessage = new EsbMessage(key.toByteArray(), this.message.toByteArray(), "topic1", 0, 100);
        List<EsbMessage> filteredMessages = this.filter.filter(Arrays.asList(esbMessage));
        assertEquals(filteredMessages.get(0), esbMessage);
    }
}
