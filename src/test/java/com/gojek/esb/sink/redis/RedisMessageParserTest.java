package com.gojek.esb.sink.redis;

import com.gojek.de.stencil.client.ClassLoadStencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.config.enums.RedisSinkType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.consumer.TestKey;
import com.gojek.esb.consumer.TestMessage;
import com.gojek.esb.consumer.TestNestedRepeatedMessage;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.sink.redis.dataentry.RedisHashSetFieldEntry;
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

    private final long bookingCustomerTotalFare = 2000L;
    private final float bookingAmountPaidByCash = 12.3F;
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
    private String bookingOrderNumber = "booking-order-1";

    @Before
    public void setUp() throws Exception {

        TestKey testKey = TestKey.newBuilder().setOrderNumber("ORDER-1-FROM-KEY").build();
        BookingLogMessage bookingMessage = BookingLogMessage.newBuilder().setOrderNumber(bookingOrderNumber).setCustomerTotalFareWithoutSurge(bookingCustomerTotalFare).setAmountPaidByCash(bookingAmountPaidByCash).build();
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order").setOrderDetails("ORDER-DETAILS").build();
        testEsbMessage = new EsbMessage(testKey.toByteArray(), testMessage.toByteArray(), "test", 1, 11);
        bookingEsbMessage = new EsbMessage(testKey.toByteArray(), bookingMessage.toByteArray(), "test", 1, 11);
        stencilClient = new ClassLoadStencilClient();
        testMessageProtoParser = new ProtoParser(stencilClient, TestMessage.class.getCanonicalName());
        bookingMessageProtoParser = new ProtoParser(stencilClient, BookingLogMessage.class.getCanonicalName());
        testKeyProtoParser = new ProtoParser(stencilClient, TestKey.class.getCanonicalName());
    }

    private void setRedisSinkConfig(String parserMode, String collectionKeyTemplate, RedisSinkType redisSinkType) {
        when(redisSinkConfig.getKafkaRecordParserMode()).thenReturn(parserMode);
        when(redisSinkConfig.getRedisKeyTemplate()).thenReturn(collectionKeyTemplate);
        when(redisSinkConfig.getRedisSinkType()).thenReturn(redisSinkType);
    }

    @Test
    public void shouldParseStringMessageForCollectionKeyTemplate() {
        setRedisSinkConfig("message", "Test-%s,1", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForTestMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("3", "details"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForTestMessage, testMessageProtoParser, redisSinkConfig);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(testEsbMessage).get(0);

        assertEquals("ORDER-DETAILS", redisHashSetFieldEntry.getValue());
        assertEquals("details", redisHashSetFieldEntry.getField());
        assertEquals("Test-test-order", redisHashSetFieldEntry.getKey());
    }

    @Test
    public void shouldParseStringMessageWithSpacesForCollectionKeyTemplate() {
        setRedisSinkConfig("message", "Test-%s, 1", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForTestMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("3", "details"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForTestMessage, testMessageProtoParser, redisSinkConfig);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(testEsbMessage).get(0);

        assertEquals("ORDER-DETAILS", redisHashSetFieldEntry.getValue());
        assertEquals("details", redisHashSetFieldEntry.getField());
        assertEquals("Test-test-order", redisHashSetFieldEntry.getKey());
    }


    @Test
    public void shouldParseFloatMessageForCollectionKeyTemplate() {
        setRedisSinkConfig("message", "Test-%.2f,16", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingEsbMessage).get(0);

        assertEquals("Test-12.30", redisHashSetFieldEntry.getKey());
        assertEquals("order_number_1", redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldParseLongMessageForCollectionKeyTemplate() {
        setRedisSinkConfig("message", "Test-%d,52", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingEsbMessage).get(0);

        assertEquals("Test-2000", redisHashSetFieldEntry.getKey());
        assertEquals("order_number_1", redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldThrowExceptionForInvalidPatternInCollectionKeyTemplate() {
        expectedException.expect(UnknownFormatConversionException.class);
        expectedException.expectMessage("Conversion = '%'");

        setRedisSinkConfig("message", "Test-%,52", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        redisMessageParser.parse(bookingEsbMessage);
    }

    @Test
    public void shouldThrowExceptionForIncompatiblePatternInCollectionKeyTemplate() {
        expectedException.expect(IllegalFormatConversionException.class);
        expectedException.expectMessage("f != java.lang.Long");

        setRedisSinkConfig("message", "Test-%f,52", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        redisMessageParser.parse(bookingEsbMessage);
    }

    @Test
    public void shouldThrowExceptionForNonExistingDescriptorInCollectionKeyTemplate() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Descriptor not found for index: 20000");

        setRedisSinkConfig("message", "Test-%f,20000", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        redisMessageParser.parse(bookingEsbMessage);
    }

    @Test
    public void shouldThrowExceptionForNullCollectionKeyTemplate() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid configuration, Collection key or key is null or empty");

        setRedisSinkConfig("message", null, RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        redisMessageParser.parse(bookingEsbMessage);
    }

    @Test
    public void shouldThrowExceptionForEmptyCollectionKeyTemplate() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid configuration, Collection key or key is null or empty");

        setRedisSinkConfig("message", "", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        redisMessageParser.parse(bookingEsbMessage);
    }

    @Test
    public void shouldAcceptStringForCollectionKey() {
        setRedisSinkConfig("message", "Test", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));

        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingEsbMessage).get(0);
        assertEquals("Test", redisHashSetFieldEntry.getKey());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldAcceptStringWithPatternForCollectionKeyWithEmptyVariables() {
        setRedisSinkConfig("message", "Test-%s", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_1"));

        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingEsbMessage).get(0);
        assertEquals("Test-%s", redisHashSetFieldEntry.getKey());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldParseLongMessageForKey() {
        setRedisSinkConfig("message", "Test-%d,52", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_%d,52"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingEsbMessage).get(0);

        assertEquals("Test-2000", redisHashSetFieldEntry.getKey());
        assertEquals(String.format("order_number_%s", bookingCustomerTotalFare), redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldParseLongMessageWithSpaceForKey() {
        setRedisSinkConfig("message", "Test-%d,52", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_%d, 52"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingEsbMessage).get(0);

        assertEquals("Test-2000", redisHashSetFieldEntry.getKey());
        assertEquals(String.format("order_number_%s", bookingCustomerTotalFare), redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldParseStringMessageForKey() {
        setRedisSinkConfig("message", "Test-%d,52", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number_%s,2"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingEsbMessage).get(0);

        assertEquals("Test-2000", redisHashSetFieldEntry.getKey());
        assertEquals(String.format("order_number_%s", bookingOrderNumber), redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldHandleStaticStringForKey() {
        setRedisSinkConfig("message", "Test-%d,52", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingEsbMessage).get(0);
        assertEquals("Test-2000", redisHashSetFieldEntry.getKey());
        assertEquals("order_number", redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldHandleStaticStringWithPatternForKey() {
        setRedisSinkConfig("message", "Test-%d,52", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number%s"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingEsbMessage).get(0);
        assertEquals("Test-2000", redisHashSetFieldEntry.getKey());
        assertEquals("order_number%s", redisHashSetFieldEntry.getField());
        assertEquals("booking-order-1", redisHashSetFieldEntry.getValue());
    }

    @Test
    public void shouldThrowErrorForInvalidFormatForKey() {
        expectedException.expect(UnknownFormatConversionException.class);
        expectedException.expectMessage("Conversion = '%");

        setRedisSinkConfig("message", "Test-%d,52", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number-%,52"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        redisMessageParser.parse(bookingEsbMessage);
    }

    @Test
    public void shouldThrowErrorForIncompatibleFormatForKey() {
        expectedException.expect(IllegalFormatConversionException.class);
        expectedException.expectMessage("d != java.lang.String");

        setRedisSinkConfig("message", "Test-%d,52", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", "order_number-%d,2"));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(testEsbMessage).get(0);

        assertEquals("ORDER-DETAILS", redisHashSetFieldEntry.getValue());
        assertEquals("details", redisHashSetFieldEntry.getField());
        assertEquals("Test-test-order", redisHashSetFieldEntry.getKey());
    }

    @Test
    public void shouldThrowExceptionForEmptyKey() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid configuration, Collection key or key is null or empty");

        setRedisSinkConfig("message", "Test-%d,52", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForBookingMessage = new ProtoToFieldMapper(testMessageProtoParser, getProperties("2", ""));
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForBookingMessage, bookingMessageProtoParser, redisSinkConfig);

        redisMessageParser.parse(bookingEsbMessage);
    }

    @Test
    public void shouldParseKeyWhenKafkaMessageParseModeSetToKey() {
        setRedisSinkConfig("key", "Test-%s,1", RedisSinkType.HASHSET);
        ProtoToFieldMapper protoToFieldMapperForKey = new ProtoToFieldMapper(testKeyProtoParser, getProperties("1", "order"));

        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForKey, testKeyProtoParser, redisSinkConfig);
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisMessageParser.parse(bookingEsbMessage).get(0);

        assertEquals(redisHashSetFieldEntry.getValue(), "ORDER-1-FROM-KEY");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowInvalidProtocolBufferExceptionWhenIncorrectProtocolUsed() {
        setRedisSinkConfig("message", "Test-%s,1", RedisSinkType.HASHSET);
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
