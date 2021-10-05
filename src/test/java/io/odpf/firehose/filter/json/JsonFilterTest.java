package io.odpf.firehose.filter.json;

import io.odpf.firehose.config.FilterConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.TestBookingLogKey;
import io.odpf.firehose.consumer.TestBookingLogMessage;
import io.odpf.firehose.consumer.TestKey;
import io.odpf.firehose.consumer.TestMessage;
import io.odpf.firehose.filter.FilterException;
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

    private FilterConfig filterConfig;
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
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JSON_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(filterConfig, instrumentation);
        List<Message> filteredMessages = jsonFilter.filter(Arrays.asList(message1, message2));
        assertEquals(1, filteredMessages.size());
        assertEquals(message1, filteredMessages.get(0));
    }

    @Test
    public void shouldFilterEsbMessagesForJsonMessageType() throws FilterException {
        Message message1 = new Message(testKeyJson1.getBytes(), testMessageJson1.getBytes(), "topic1", 0, 100);
        Message message2 = new Message(testKeyJson2.getBytes(), testMessageJson2.getBytes(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_JSON_ESB_MESSAGE_FORMAT", "JSON");
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(filterConfig, instrumentation);
        List<Message> filteredMessages = jsonFilter.filter(Arrays.asList(message1, message2));
        assertEquals(1, filteredMessages.size());
        assertEquals(message1, filteredMessages.get(0));
    }

    @Test
    public void shouldNotFilterProtobufMessagesWhenEmptyJSONSchema() throws FilterException {
        Message message1 = new Message(testKeyProto1.toByteArray(), testMessageProto1.toByteArray(), "topic1", 0, 100);
        Message message2 = new Message(testKeyProto2.toByteArray(), testMessageProto2.toByteArray(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JSON_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(filterConfig, instrumentation);
        List<Message> inputMessages = Arrays.asList(message1, message2);
        List<Message> filteredMessages = jsonFilter.filter(inputMessages);
        assertEquals(inputMessages, filteredMessages);
    }

    @Test
    public void shouldNotFilterJsonMessagesWhenEmptyJSONSchema() throws FilterException {
        Message message1 = new Message(testKeyJson1.getBytes(), testMessageJson1.getBytes(), "topic1", 0, 100);
        Message message2 = new Message(testKeyJson2.getBytes(), testMessageJson2.getBytes(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_JSON_ESB_MESSAGE_FORMAT", "JSON");
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JSON_SCHEMA", "");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(filterConfig, instrumentation);
        List<Message> inputMessages = Arrays.asList(message1, message2);
        List<Message> filteredMessages = jsonFilter.filter(inputMessages);
        assertEquals(inputMessages, filteredMessages);
    }


    @Test
    public void shouldNotFilterEsbMessagesForEmptyBooleanValuesForProtobufMessageType() throws FilterException {
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setCustomerId("customerId").build();
        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().build();
        Message message = new Message(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray(), "topic1", 0, 100);
        HashMap<String, String> bookingFilterConfigs = new HashMap<>();
        bookingFilterConfigs.put("FILTER_DATA_SOURCE", "message");
        bookingFilterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"customer_dynamic_surge_enabled\":{\"const\":\"true\"}}}");
        bookingFilterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestBookingLogMessage.class.getName());
        bookingFilterConfigs.put("FILTER_JSON_ESB_MESSAGE_FORMAT", "PROTOBUF");
        FilterConfig bookingConsumerConfig = ConfigFactory.create(FilterConfig.class, bookingFilterConfigs);
        JsonFilter bookingFilter = new JsonFilter(bookingConsumerConfig, instrumentation);
        List<Message> filteredMessages = bookingFilter.filter(Collections.singletonList(message));
        assertEquals(message, filteredMessages.get(0));
    }


    @Test
    public void shouldThrowExceptionWhenJsonMessageInvalid() throws FilterException {
        Message message1 = new Message(new byte[]{1, 2}, testMessageJson1.getBytes(), "topic1", 0, 100);
        Message message2 = new Message(testKeyJson2.getBytes(), testMessageJson2.getBytes(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_JSON_ESB_MESSAGE_FORMAT", "JSON");
        filterConfigs.put("FILTER_DATA_SOURCE", "key");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(filterConfig, instrumentation);
        thrown.expect(FilterException.class);
        thrown.expectMessage("Failed to parse JSON message");
        jsonFilter.filter(Arrays.asList(message1, message2));
    }

    @Test
    public void shouldThrowExceptionWhenProtobufMessageInvalid() throws FilterException {
        Message message1 = new Message(new byte[]{1, 2}, testMessageProto1.toByteArray(), "topic1", 0, 100);
        Message message2 = new Message(testKeyProto2.toByteArray(), testMessageProto2.toByteArray(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "key");
        filterConfigs.put("FILTER_JSON_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(filterConfig, instrumentation);
        thrown.expect(FilterException.class);
        thrown.expectMessage("Failed to parse Protobuf message");
        jsonFilter.filter(Arrays.asList(message1, message2));
    }

    @Test
    public void shouldThrowExceptionWhenProtoSchemaClassInvalid() throws FilterException {
        Message message1 = new Message(testKeyProto1.toByteArray(), testMessageProto1.toByteArray(), "topic1", 0, 100);
        Message message2 = new Message(testKeyProto2.toByteArray(), testMessageProto2.toByteArray(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "key");
        filterConfigs.put("FILTER_JSON_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", "ss");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(filterConfig, instrumentation);
        thrown.expect(FilterException.class);
        thrown.expectMessage("Proto schema class is invalid");
        jsonFilter.filter(Arrays.asList(message1, message2));
    }

    @Test
    public void shouldLogCauseToFilterOutMessageForProtobufMessageFormat() throws FilterException {
        Message message1 = new Message(testKeyProto1.toByteArray(), testMessageProto1.toByteArray(), "topic1", 0, 100);
        Message message2 = new Message(testKeyProto2.toByteArray(), testMessageProto2.toByteArray(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JSON_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(filterConfig, instrumentation);
        jsonFilter.filter(Arrays.asList(message1, message2));
        verify(instrumentation, times(1)).logDebug("Message filtered out due to: {}", "$.order_number: must be a constant value 123");
    }

    @Test
    public void shouldLogCauseToFilterOutMessageForJsonMessageFormat() throws FilterException {
        Message message1 = new Message(testKeyJson1.getBytes(), testMessageJson1.getBytes(), "topic1", 0, 100);
        Message message2 = new Message(testKeyJson2.getBytes(), testMessageJson2.getBytes(), "topic1", 0, 101);
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_JSON_ESB_MESSAGE_FORMAT", "JSON");
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        jsonFilter = new JsonFilter(filterConfig, instrumentation);
        jsonFilter.filter(Arrays.asList(message1, message2));
        verify(instrumentation, times(1)).logDebug("Message filtered out due to: {}", "$.order_number: must be a constant value 123");
    }
}
