package com.gojek.esb.sink.redis;

import com.gojek.de.stencil.client.ClassLoadStencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.consumer.TestKey;
import com.gojek.esb.consumer.TestMessage;
import com.gojek.esb.consumer.TestNestedRepeatedMessage;
import com.gojek.esb.proto.ProtoToFieldMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.IllegalFormatConversionException;
import java.util.Properties;
import java.util.UnknownFormatConversionException;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisMessageParserTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private RedisSinkConfig redisSinkConfig;
    private EsbMessage testEsbMessage;
    private ProtoParser testKeyProtoParser;
    private ProtoParser testMessageProtoParser;
    private ClassLoadStencilClient stencilClient;
    private EsbMessage bookingEsbMessage;
    private ProtoParser bookingMessageProtoParser;

    @Before
    public void setUp() throws Exception {

        TestKey testKey = TestKey.newBuilder().setOrderNumber("ORDER-1-FROM-KEY").build();
        BookingLogMessage bookingMessage = BookingLogMessage.newBuilder().setOrderNumber("booking-order-1").setCustomerTotalFareWithoutSurge(2000L).setAmountPaidByCash(12.3F).build();
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order").setOrderDetails("ORDER-DETAILS").build();
        testEsbMessage = new EsbMessage(testKey.toByteArray(), testMessage.toByteArray(), "test", 1, 11);
        bookingEsbMessage = new EsbMessage(testKey.toByteArray(), bookingMessage.toByteArray(), "test", 1, 11);
        stencilClient = new ClassLoadStencilClient();
        testMessageProtoParser = new ProtoParser(stencilClient, TestMessage.class.getCanonicalName());
        bookingMessageProtoParser = new ProtoParser(stencilClient, BookingLogMessage.class.getCanonicalName());
        testKeyProtoParser = new ProtoParser(stencilClient, TestKey.class.getCanonicalName());
    }

    private void setRedisSinkConfig(String parserMode, String collectionKeyPattern, String collectionKeyVariables) {
        when(redisSinkConfig.getKafkaRecordParserMode()).thenReturn(parserMode);
        when(redisSinkConfig.getRedisKeyPattern()).thenReturn(collectionKeyPattern);
        when(redisSinkConfig.getRedisKeyVariables()).thenReturn(collectionKeyVariables);
    }

    @Test
    public void shouldParseStringMessageForCollectionKey() {
        setRedisSinkConfig("message", "Test-%s", "1");
        ProtoToFieldMapper protoToFieldMapperForTestMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("3", "details"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForTestMessage, testMessageProtoParser, redisSinkConfig);

        RedisHashSetFieldEntry redisHashSetFieldEntry = redisMessageParser.parse(testEsbMessage).get(0);

        assertEquals("ORDER-DETAILS", redisHashSetFieldEntry.getValue());
        assertEquals("details", redisHashSetFieldEntry.getField());
        assertEquals("Test-test-order", redisHashSetFieldEntry.getKey());
    }

    @Test
    public void shouldParseFloatMessageForCollectionKey() {
        setRedisSinkConfig("message", "Test-%.2f", "16");
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        RedisHashSetFieldEntry redisHashSetFieldEntry = redisMessageParser.parse(bookingEsbMessage).get(0);

        assertEquals("Test-12.30", redisHashSetFieldEntry.getKey());
        assertEquals("order_number_1", redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldParseLongMessageForCollectionKey() {
        setRedisSinkConfig("message", "Test-%d", "52");
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        RedisHashSetFieldEntry redisHashSetFieldEntry = redisMessageParser.parse(bookingEsbMessage).get(0);

        assertEquals("Test-2000", redisHashSetFieldEntry.getKey());
        assertEquals("order_number_1", redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldThrowExceptionForInvalidPatternInCollectionKey() {
        expectedException.expect(UnknownFormatConversionException.class);
        expectedException.expectMessage("Conversion = '%'");

        setRedisSinkConfig("message", "Test-%", "52");
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        redisMessageParser.parse(bookingEsbMessage);
    }

    @Test
    public void shouldThrowExceptionForIncompatiblePatternInCollectionKey() {
        expectedException.expect(IllegalFormatConversionException.class);
        expectedException.expectMessage("f != java.lang.Long");

        setRedisSinkConfig("message", "Test-%f", "52");
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        redisMessageParser.parse(bookingEsbMessage);
    }

    @Test
    public void shouldThrowExceptionForNonExistingDescriptorInCollectionKeyVariable() {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Descriptor not found for index: 20000");

        setRedisSinkConfig("message", "Test-%f", "20000");
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        redisMessageParser.parse(bookingEsbMessage);
    }

    @Test
    public void shouldAcceptStringForCollectionKey() {
        setRedisSinkConfig("message", "Test", "");
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));

        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        assertEquals("Test", redisMessageParser.parse(bookingEsbMessage).get(0).getKey());
        assertEquals("booking-order-1", redisMessageParser.parse(bookingEsbMessage).get(0).getValue());
    }

    @Test
    public void shouldParseKeyWhenKafkaMessageParseModeSetToKey() {
        setRedisSinkConfig("key", "Test-%s", "1");
        ProtoToFieldMapper protoToFieldMapperForKey = new ProtoToFieldMapper(testKeyProtoParser, getProperties("1", "order"));

        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForKey, testKeyProtoParser, redisSinkConfig);

        assertEquals(redisMessageParser.parse(testEsbMessage).get(0).getValue(), "ORDER-1-FROM-KEY");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowInvalidProtocolBufferExceptionWhenIncorrectProtocolUsed() {
        setRedisSinkConfig("message", "Test-%s", "1");
        ProtoParser protoParserForTest = new ProtoParser(stencilClient, TestNestedRepeatedMessage.class.getCanonicalName());
        ProtoToFieldMapper protoToFieldMapperForTest = new ProtoToFieldMapper(protoParserForTest, getProperties("3", "details"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForTest, protoParserForTest, redisSinkConfig);

        redisMessageParser.parse(testEsbMessage);
    }


    private Properties getProperties(String s, String order) {
        Properties propertiesForKey = new Properties();
        propertiesForKey.setProperty(s, order);
        return propertiesForKey;
    }
}
