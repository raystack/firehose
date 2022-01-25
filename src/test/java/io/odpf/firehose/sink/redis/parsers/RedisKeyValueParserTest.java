package io.odpf.firehose.sink.redis.parsers;

import io.odpf.firehose.config.RedisSinkConfig;
import io.odpf.firehose.consumer.TestKey;
import io.odpf.firehose.consumer.TestMessage;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.redis.dataentry.RedisDataEntry;
import io.odpf.firehose.sink.redis.dataentry.RedisKeyValueEntry;
import io.odpf.stencil.Parser;
import io.odpf.stencil.client.ClassLoadStencilClient;
import io.odpf.stencil.client.StencilClient;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.aeonbits.owner.ConfigFactory.create;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class RedisKeyValueParserTest {

    private final byte[] testKeyByteArr = TestKey.newBuilder()
            .setOrderNumber("order-2")
            .setOrderUrl("order-url-world")
            .build()
            .toByteArray();
    private StatsDReporter statsDReporter;
    private StencilClient stencilClient = new ClassLoadStencilClient();
    private Parser testMessageProtoParser = stencilClient.getParser(TestMessage.class.getCanonicalName());
    private Parser testKeyProtoParser = stencilClient.getParser(TestKey.class.getCanonicalName());

    @Test
    public void parse() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("KAFKA_RECORD_PARSER_MODE", "message");
            put("SINK_REDIS_KEY_TEMPLATE", "hello_world_%%s,1");
            put("SINK_REDIS_KEY_VALUE_DATA_PROTO_INDEX", "3");
        }};
        RedisSinkConfig redisSinkConfig = create(RedisSinkConfig.class, config);
        RedisKeyValueParser redisKeyValueParser = new RedisKeyValueParser(testMessageProtoParser, redisSinkConfig, statsDReporter);
        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("new-eureka-order")
                .build()
                .toByteArray();
        Message message = new Message(null, logMessage, "test-topic", 1, 100);
        List<RedisDataEntry> redisDataEntries = redisKeyValueParser.parse(message);

        RedisKeyValueEntry expectedEntry = new RedisKeyValueEntry("hello_world_xyz-order", "new-eureka-order", null);
        assertEquals(asList(expectedEntry), redisDataEntries);

    }

    @Test
    public void shouldParseWhenUsingModeKey() {
        Map<String, String> config = new HashMap<String, String>() {{
            put("KAFKA_RECORD_PARSER_MODE", "key");
            put("SINK_REDIS_KEY_TEMPLATE", "hello_world_%%s,1");
            put("SINK_REDIS_KEY_VALUE_DATA_PROTO_INDEX", "2");
        }};
        RedisSinkConfig redisSinkConfig = create(RedisSinkConfig.class, config);

        RedisKeyValueParser redisKeyValueParser = new RedisKeyValueParser(testKeyProtoParser, redisSinkConfig, null);
        Message message = new Message(testKeyByteArr, null, null, 0, 0L);
        redisKeyValueParser.parse(message);
    }

    @Test
    public void shouldThrowExceptionWhenKeyTemplateIsEmpty() {

        Message message = new Message(testKeyByteArr, testKeyByteArr, "", 0, 0);
        RedisSinkConfig redisSinkConfig = create(RedisSinkConfig.class, singletonMap("SINK_REDIS_KEY_TEMPLATE", ""));
        RedisKeyValueParser redisKeyValueParser = new RedisKeyValueParser(testKeyProtoParser, redisSinkConfig, null);
        IllegalArgumentException illegalArgumentException =
                assertThrows(IllegalArgumentException.class, () -> redisKeyValueParser.parse(message));
        assertEquals("Template '' is invalid", illegalArgumentException.getMessage());
    }

    @Test
    public void shouldThrowExceptionForNoListProtoIndex() {
        HashMap<String, String> config = new HashMap<String, String>() {{
            put("SINK_REDIS_KEY_TEMPLATE", "hello_world%%s,1");
        }};
        RedisSinkConfig redisSinkConfig = create(RedisSinkConfig.class, config);

        Message message = new Message(testKeyByteArr, testKeyByteArr, "", 0, 0);
        RedisKeyValueParser redisKeyValueParser = new RedisKeyValueParser(testKeyProtoParser, redisSinkConfig, null);
        IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class,
                () -> redisKeyValueParser.parse(message));
        assertEquals("Please provide SINK_REDIS_KEY_VALUE_DATA_PROTO_INDEX in key value sink", illegalArgumentException.getMessage());
    }
}
