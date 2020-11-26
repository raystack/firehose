package com.gojek.esb.filter;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.config.enums.EsbFilterType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.consumer.TestBookingLogKey;
import com.gojek.esb.consumer.TestBookingLogMessage;
import com.gojek.esb.consumer.TestKey;
import com.gojek.esb.consumer.TestMessage;
import com.gojek.esb.metrics.Instrumentation;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

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

    @Mock
    private Instrumentation instrumentation;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_TYPE", "message");
        filterConfigs.put("FILTER_EXPRESSION", "testMessage.getOrderNumber() == 123");
        filterConfigs.put("FILTER_PROTO_SCHEMA", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        message = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
    }

    @Test
    public void shouldFilterEsbMessages() throws EsbFilterException {
        EsbMessage esbMessage = new EsbMessage(key.toByteArray(), this.message.toByteArray(), "topic1", 0, 100);
        filter = new EsbMessageFilter(kafkaConsumerConfig, instrumentation);
        List<EsbMessage> filteredMessages = filter.filter(Arrays.asList(esbMessage));
        assertEquals(filteredMessages.get(0), esbMessage);
    }

    @Test
    public void shouldNotFilterEsbMessagesForEmptyBooleanValues() throws EsbFilterException {
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setCustomerId("customerId").build();
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().build();
        EsbMessage esbMessage = new EsbMessage(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray(), "topic1", 0, 100);
        HashMap<String, String> bookingFilterConfigs = new HashMap<>();
        bookingFilterConfigs.put("FILTER_TYPE", "message");
        bookingFilterConfigs.put("FILTER_EXPRESSION", "testBookingLogMessage.getCustomerDynamicSurgeEnabled() == false");
        bookingFilterConfigs.put("FILTER_PROTO_SCHEMA", TestBookingLogMessage.class.getName());
        KafkaConsumerConfig bookingConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, bookingFilterConfigs);
        EsbMessageFilter bookingFilter = new EsbMessageFilter(bookingConsumerConfig, instrumentation);
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

        filter = new EsbMessageFilter(kafkaConsumerConfig, instrumentation);
        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        message = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();

        EsbMessage esbMessage = new EsbMessage(key.toByteArray(), this.message.toByteArray(), "topic1", 0, 100);
        filter.filter(Arrays.asList(esbMessage));
    }

    @Test
    public void shouldNotApplyFilterOnEmptyFilterType() throws EsbFilterException {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_EXPRESSION", "testMessage.getOrderNumber() == 123");
        filterConfigs.put("FILTER_PROTO_SCHEMA", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        filter = new EsbMessageFilter(kafkaConsumerConfig, instrumentation);
        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        message = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();

        EsbMessage esbMessage = new EsbMessage(key.toByteArray(), this.message.toByteArray(), "topic1", 0, 100);
        List<EsbMessage> filteredMessages = this.filter.filter(Arrays.asList(esbMessage));
        assertEquals(filteredMessages.get(0), esbMessage);
    }

    @Test
    public void shouldLogFilterTypeIfFilterTypeIsNotNone() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_TYPE", "message");
        filterConfigs.put("FILTER_EXPRESSION", "testMessage.getOrderNumber() == 123");
        filterConfigs.put("FILTER_PROTO_SCHEMA", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        new EsbMessageFilter(kafkaConsumerConfig, instrumentation);
        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("\n\tFilter type: {}", EsbFilterType.MESSAGE);
        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("\n\tFilter schema: {}", TestMessage.class.getName());
        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("\n\tFilter expression: {}", "testMessage.getOrderNumber() == 123");
    }

    @Test
    public void shouldLogFilterTypeIfFilterTypeIsNone() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_TYPE", "none");
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        new EsbMessageFilter(kafkaConsumerConfig, instrumentation);
        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("No filter is selected");
    }
}
