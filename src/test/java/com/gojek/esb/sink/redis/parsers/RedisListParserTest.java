package com.gojek.esb.sink.redis.parsers;

import com.gojek.de.stencil.client.ClassLoadStencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.config.enums.RedisSinkType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.consumer.TestBookingLogMessage;
import com.gojek.esb.consumer.TestKey;
import com.gojek.esb.consumer.TestMessage;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.redis.dataentry.RedisListEntry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisListParserTest {
    private final long bookingCustomerTotalFare = 2000L;
    private final float bookingAmountPaidByCash = 12.3F;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private RedisSinkConfig redisSinkConfig;

    @Mock
    private StatsDReporter statsDReporter;

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
        TestBookingLogMessage bookingMessage = TestBookingLogMessage.newBuilder().setOrderNumber(bookingOrderNumber).setCustomerTotalFareWithoutSurge(bookingCustomerTotalFare).setAmountPaidByCash(bookingAmountPaidByCash).build();
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order").setOrderDetails("ORDER-DETAILS").build();
        testEsbMessage = new EsbMessage(testKey.toByteArray(), testMessage.toByteArray(), "test", 1, 11);
        bookingEsbMessage = new EsbMessage(testKey.toByteArray(), bookingMessage.toByteArray(), "test", 1, 11);
        stencilClient = new ClassLoadStencilClient();
        testMessageProtoParser = new ProtoParser(stencilClient, TestMessage.class.getCanonicalName());
        bookingMessageProtoParser = new ProtoParser(stencilClient, TestBookingLogMessage.class.getCanonicalName());
        testKeyProtoParser = new ProtoParser(stencilClient, TestKey.class.getCanonicalName());
    }

    private void setRedisSinkConfig(String parserMode, String collectionKeyTemplate, RedisSinkType redisSinkType) {
        when(redisSinkConfig.getKafkaRecordParserMode()).thenReturn(parserMode);
        when(redisSinkConfig.getRedisKeyTemplate()).thenReturn(collectionKeyTemplate);
        when(redisSinkConfig.getRedisListDataProtoIndex()).thenReturn("1");
    }

    @Test
    public void shouldParseStringMessageForCollectionKeyTemplateInList() {
        setRedisSinkConfig("message", "Test-%s,1", RedisSinkType.LIST);
        RedisParser redisParser = new RedisListParser(testMessageProtoParser, redisSinkConfig, statsDReporter);

        RedisListEntry redisListEntry = (RedisListEntry) redisParser.parse(testEsbMessage).get(0);

        assertEquals("test-order", redisListEntry.getValue());
        assertEquals("Test-test-order", redisListEntry.getKey());
    }

    @Test
    public void shouldParseKeyWhenKafkaMessageParseModeSetToKey() {
        setRedisSinkConfig("key", "Test-%s,1", RedisSinkType.LIST);

        RedisParser redisParser = new RedisListParser(testKeyProtoParser, redisSinkConfig, statsDReporter);
        RedisListEntry redisListEntry = (RedisListEntry) redisParser.parse(bookingEsbMessage).get(0);

        assertEquals(redisListEntry.getValue(), "ORDER-1-FROM-KEY");
    }

    @Test
    public void shouldThrowExceptionForEmptyKey() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Template '' is invalid");

        setRedisSinkConfig("message", "", RedisSinkType.LIST);
        RedisParser redisParser = new RedisListParser(bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        redisParser.parse(bookingEsbMessage);
    }

    @Test
    public void shouldThrowExceptionForNoListProtoIndex() {
        setRedisSinkConfig("message", "Test-%s,1", RedisSinkType.LIST);
        when(redisSinkConfig.getRedisListDataProtoIndex()).thenReturn(null);
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Please provide REDIS_LIST_DATA_PROTO_INDEX in list sink");

        RedisParser redisParser = new RedisListParser(bookingMessageProtoParser, redisSinkConfig, statsDReporter);

        redisParser.parse(bookingEsbMessage);
    }
}
