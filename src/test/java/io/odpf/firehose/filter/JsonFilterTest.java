package io.odpf.firehose.filter;

import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.config.enums.FilterDataSourceType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.TestBookingLogKey;
import io.odpf.firehose.consumer.TestBookingLogMessage;
import io.odpf.firehose.consumer.TestKey;
import io.odpf.firehose.consumer.TestMessage;
import io.odpf.firehose.metrics.Instrumentation;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class JsonFilterTest {

    private KafkaConsumerConfig kafkaConsumerConfig;

    private JsonFilter jsonFilter;

    private TestMessage testMessageProto1, testMessageProto2;
    private TestKey testKeyProto1, testKeyProto2;
    private String testMessageJson1, testMessageJson2;
    private String testKeyJson1, testKeyJson2;


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private Instrumentation instrumentation;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        testKeyProto1 = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();
        testMessageProto1 = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();

        testKeyJson1 = "{\"order_number\":\"123\",\"order_url\":\"abc\"}";
        testMessageJson1 = "{\"order_number\":\"123\",\"order_url\":\"abc\",\"order_details\":\"details\"}";

        testKeyProto2 = TestKey.newBuilder().setOrderNumber("92").setOrderUrl("pqr").build();
        testMessageProto2 = TestMessage.newBuilder().setOrderNumber("92").setOrderUrl("pqr").setOrderDetails("details").build();

        testKeyJson2 = "{\"order_number\":\"92\",\"order_url\":\"pqr\"}";
        testMessageJson2 = "{\"order_number\":\"92\",\"order_url\":\"pqr\",\"order_details\":\"details\"}";

    }

    @Test
    public void shouldFilterEsbMessagesForProtobufMessageType() throws FilterException {
        Message message1 = new Message(testKeyProto1.toByteArray(), testMessageProto1.toByteArray(), "topic1", 0, 100);
        Message message2 = new Message(testKeyProto2.toByteArray(), testMessageProto2.toByteArray(), "topic1", 0, 101);

        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_JSON_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_ESB_MESSAGE_TYPE", "PROTOBUF");

        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_JSON_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        jsonFilter = new JsonFilter(kafkaConsumerConfig, instrumentation);

        List<Message> filteredMessages = jsonFilter.filter(Arrays.asList(message1, message2));
        assertEquals(filteredMessages.size(), 1);
        assertEquals(filteredMessages.get(0), message1);
    }

    @Test
    public void shouldNotFilterEsbMessagesForEmptyBooleanValuesForProtobufMessageType() throws FilterException {
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setCustomerId("customerId").build();
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().build();
        Message message = new Message(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray(), "topic1", 0, 100);
        HashMap<String, String> bookingFilterConfigs = new HashMap<>();

        bookingFilterConfigs.put("FILTER_JSON_DATA_SOURCE", "message");
        bookingFilterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"customer_dynamic_surge_enabled\":{\"const\":\"true\"}}}");
        bookingFilterConfigs.put("FILTER_JSON_SCHEMA_PROTO_CLASS", TestBookingLogMessage.class.getName());
        bookingFilterConfigs.put("FILTER_ESB_MESSAGE_TYPE", "PROTOBUF");

        KafkaConsumerConfig bookingConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, bookingFilterConfigs);
        JsonFilter bookingFilter = new JsonFilter(bookingConsumerConfig, instrumentation);
        List<Message> filteredMessages = bookingFilter.filter(Collections.singletonList(message));
        assertEquals(filteredMessages.get(0), message);
    }

    @Test
    public void shouldThrowExceptionOnInvalidFilterSchemaForProtobufMessageType() throws FilterException {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_JSON_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JSON_SCHEMA", "12/s");
        filterConfigs.put("FILTER_JSON_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfigs.put("FILTER_ESB_MESSAGE_TYPE", "PROTOBUF");

        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        jsonFilter = new JsonFilter(kafkaConsumerConfig, instrumentation);

        Message message = new Message(testKeyProto1.toByteArray(), testMessageProto1.toByteArray(), "topic1", 0, 100);
        thrown.expect(FilterException.class);
        jsonFilter.filter(Collections.singletonList(message));
    }

    @Test
    public void shouldNotApplyFilterOnEmptyFilterDataSourceForProtobufMessageType() throws FilterException {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_JSON_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfigs.put("FILTER_ESB_MESSAGE_TYPE", "PROTOBUF");

        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        jsonFilter = new JsonFilter(kafkaConsumerConfig, instrumentation);

        Message message = new Message(testKeyProto1.toByteArray(), testMessageProto1.toByteArray(), "topic1", 0, 100);
        List<Message> filteredMessages = this.jsonFilter.filter(Collections.singletonList(message));
        assertEquals(filteredMessages.get(0), message);
    }

    @Test
    public void shouldLogFilterTypeIfFilterDataSourceIsNotNoneForProtobufMessageType() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_JSON_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_ESB_MESSAGE_TYPE", "PROTOBUF");

        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_JSON_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        new JsonFilter(kafkaConsumerConfig, instrumentation);
        verify(instrumentation, times(1)).logInfo("\n\tFilter type: {}", FilterDataSourceType.MESSAGE);
        verify(instrumentation, times(1)).logInfo("\n\tMessage Proto class: {}", TestMessage.class.getName());
        verify(instrumentation, times(1)).logInfo("\n\tFilter JSON Schema: {}", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
    }


    @Test
    public void shouldFilterEsbMessagesForJsonMessageType() throws FilterException {
        Message message1 = new Message(testKeyJson1.getBytes(), testMessageJson1.getBytes(), "topic1", 0, 100);
        Message message2 = new Message(testKeyJson2.getBytes(), testMessageJson2.getBytes(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();

        filterConfigs.put("FILTER_ESB_MESSAGE_TYPE", "JSON");
        filterConfigs.put("FILTER_JSON_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");

        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(kafkaConsumerConfig, instrumentation);
        List<Message> filteredMessages = jsonFilter.filter(Arrays.asList(message1, message2));
        assertEquals(filteredMessages.size(), 1);
        assertEquals(filteredMessages.get(0), message1);
    }

    @Test
    public void shouldNotFilterEsbMessagesForEmptyBooleanValuesForJsonMessageType() throws FilterException {
        String bookingLogMessageJson = "{\"customer_id\":\"customerid\"}";
        String bookingLogKeyJson = "";
        Message message = new Message(bookingLogKeyJson.getBytes(), bookingLogMessageJson.getBytes(), "topic1", 0, 100);
        HashMap<String, String> bookingFilterConfigs = new HashMap<>();
        bookingFilterConfigs.put("FILTER_ESB_MESSAGE_TYPE", "JSON");

        bookingFilterConfigs.put("FILTER_JSON_DATA_SOURCE", "message");
        bookingFilterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"customer_dynamic_surge_enabled\":{\"const\":\"true\"}}}");

        KafkaConsumerConfig bookingConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, bookingFilterConfigs);
        JsonFilter bookingFilter = new JsonFilter(bookingConsumerConfig, instrumentation);
        List<Message> filteredMessages = bookingFilter.filter(Collections.singletonList(message));
        assertEquals(filteredMessages.get(0), message);
    }

    @Test
    public void shouldThrowExceptionOnInvalidFilterSchemaForJsonMessageType() throws FilterException {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_JSON_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JSON_SCHEMA", "12/s");
        filterConfigs.put("FILTER_ESB_MESSAGE_TYPE", "JSON");

        filterConfigs.put("FILTER_JSON_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        jsonFilter = new JsonFilter(kafkaConsumerConfig, instrumentation);

        Message message = new Message(testKeyJson1.getBytes(), testMessageJson1.getBytes(), "topic1", 0, 100);
        thrown.expect(FilterException.class);

        jsonFilter.filter(Collections.singletonList(message));
    }

    @Test
    public void shouldNotApplyFilterOnEmptyFilterDataSourceForJsonMessageType() throws FilterException {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"1253\"}}}");
        filterConfigs.put("FILTER_ESB_MESSAGE_TYPE", "JSON");

        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(kafkaConsumerConfig, instrumentation);

        Message message = new Message(testKeyJson1.getBytes(), testMessageJson1.getBytes(), "topic1", 0, 100);
        List<Message> filteredMessages = jsonFilter.filter(Collections.singletonList(message));
        assertEquals(filteredMessages.get(0), message);
    }


    @Test
    public void shouldLogFilterTypeIfFilterTypeIsNone() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_JSON_DATA_SOURCE", "none");
        kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, filterConfigs);

        new JsonFilter(kafkaConsumerConfig, instrumentation);
        verify(instrumentation, times(1)).logInfo("No filter is selected");
    }
}
