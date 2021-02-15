package com.gojek.esb.filter;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.config.enums.FilterType;
import com.gojek.esb.consumer.Message;
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

public class MessageFilterTest {

    private KafkaConsumerConfig kafkaConsumerConfig;

    private Filter filter;

    private TestMessage testMessage;
    private TestKey key;

    @Mock
    private Instrumentation instrumentation;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("filter.type", "message");
        filterConfigs.put("filter.expression", "testMessage.getOrderNumber() == 123");
        filterConfigs.put("filter.proto.schema", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        testMessage = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
    }

    @Test
    public void shouldFilterEsbMessages() throws FilterException {
        Message message = new Message(key.toByteArray(), this.testMessage.toByteArray(), "topic1", 0, 100);
        filter = new MessageFilter(kafkaConsumerConfig, instrumentation);
        List<Message> filteredMessages = filter.filter(Arrays.asList(message));
        assertEquals(filteredMessages.get(0), message);
    }

    @Test
    public void shouldNotFilterEsbMessagesForEmptyBooleanValues() throws FilterException {
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setCustomerId("customerId").build();
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().build();
        Message message = new Message(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray(), "topic1", 0, 100);
        HashMap<String, String> bookingFilterConfigs = new HashMap<>();
        bookingFilterConfigs.put("filter.type", "message");
        bookingFilterConfigs.put("filter.expression", "testBookingLogMessage.getCustomerDynamicSurgeEnabled() == false");
        bookingFilterConfigs.put("filter.proto.schema", TestBookingLogMessage.class.getName());
        KafkaConsumerConfig bookingConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, bookingFilterConfigs);
        MessageFilter bookingFilter = new MessageFilter(bookingConsumerConfig, instrumentation);
        List<Message> filteredMessages = bookingFilter.filter(Arrays.asList(message));
        assertEquals(filteredMessages.get(0), message);
    }

    @Test(expected = FilterException.class)
    public void shouldThrowExceptionOnInvalidFilterExpression() throws FilterException {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("filter.type", "message");
        filterConfigs.put("filter.expression", "1+2");
        filterConfigs.put("filter.proto.schema", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        filter = new MessageFilter(kafkaConsumerConfig, instrumentation);
        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        this.testMessage = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();

        Message message = new Message(key.toByteArray(), this.testMessage.toByteArray(), "topic1", 0, 100);
        filter.filter(Arrays.asList(message));
    }

    @Test
    public void shouldNotApplyFilterOnEmptyFilterType() throws FilterException {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("filter.expression", "testMessage.getOrderNumber() == 123");
        filterConfigs.put("filter.proto.schema", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        filter = new MessageFilter(kafkaConsumerConfig, instrumentation);
        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        this.testMessage = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();

        Message message = new Message(key.toByteArray(), this.testMessage.toByteArray(), "topic1", 0, 100);
        List<Message> filteredMessages = this.filter.filter(Arrays.asList(message));
        assertEquals(filteredMessages.get(0), message);
    }

    @Test
    public void shouldLogFilterTypeIfFilterTypeIsNotNone() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("filter.type", "message");
        filterConfigs.put("filter.expression", "testMessage.getOrderNumber() == 123");
        filterConfigs.put("filter.proto.schema", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        new MessageFilter(kafkaConsumerConfig, instrumentation);
        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("\n\tFilter type: {}", FilterType.MESSAGE);
        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("\n\tFilter schema: {}", TestMessage.class.getName());
        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("\n\tFilter expression: {}", "testMessage.getOrderNumber() == 123");
    }

    @Test
    public void shouldLogFilterTypeIfFilterTypeIsNone() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("filter.type", "none");
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        new MessageFilter(kafkaConsumerConfig, instrumentation);
        Mockito.verify(instrumentation, Mockito.times(1)).logInfo("No filter is selected");
    }
}
