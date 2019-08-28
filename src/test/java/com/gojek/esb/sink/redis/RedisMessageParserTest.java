package com.gojek.esb.sink.redis;

import com.gojek.de.stencil.client.ClassLoadStencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.consumer.TestKey;
import com.gojek.esb.consumer.TestMessage;
import com.gojek.esb.consumer.TestNestedRepeatedMessage;
import com.gojek.esb.proto.ProtoToFieldMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Properties;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisMessageParserTest {

    @Mock
    private RedisSinkConfig redisSinkConfig;

    private EsbMessage esbMessage;
    private ProtoToFieldMapper protoToFieldMapperForMessage;
    private ProtoToFieldMapper protoToFieldMapperForKey;
    private ProtoParser protoParserForKey;
    private ProtoParser protoParserForMessage;
    private ClassLoadStencilClient stencilClient;

    @Before
    public void setUp() throws Exception {

        TestKey testKey = TestKey.newBuilder().setOrderNumber("ORDER-1-FROM-KEY").build();
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("ORDER-1").setOrderDetails("ORDER-DETAILS").build();

        esbMessage = new EsbMessage(testKey.toByteArray(), testMessage.toByteArray(), "test", 1, 11);

        stencilClient = new ClassLoadStencilClient();

        protoParserForMessage = new ProtoParser(stencilClient, TestMessage.class.getCanonicalName());
        protoParserForKey = new ProtoParser(stencilClient, TestKey.class.getCanonicalName());

        Properties propertiesForMessage = new Properties();
        propertiesForMessage.setProperty("3", "details");
        Properties propertiesForKey = new Properties();
        propertiesForKey.setProperty("1", "order");

        protoToFieldMapperForMessage = new ProtoToFieldMapper(protoParserForMessage, propertiesForMessage);
        protoToFieldMapperForKey = new ProtoToFieldMapper(protoParserForKey, propertiesForKey);

        when(redisSinkConfig.getKafkaRecordParserMode()).thenReturn("message");
        when(redisSinkConfig.getRedisKeyProtoIndex()).thenReturn(1);
    }

    @Test
    public void shouldParseMessageByDefault() {
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForMessage, protoParserForMessage, redisSinkConfig);
        assertEquals(redisMessageParser.parse(esbMessage).get(0).getValue(), "ORDER-DETAILS");
    }

    @Test
    public void shouldParseKeyWhenKafkaMessageParseModeSetToKey() {
        when(redisSinkConfig.getKafkaRecordParserMode()).thenReturn("key");
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForKey, protoParserForKey, redisSinkConfig);

        assertEquals(redisMessageParser.parse(esbMessage).get(0).getValue(), "ORDER-1-FROM-KEY");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowInvalidProtocolBufferExceptionWhenIncorrectProtocolUsed() {
        ProtoParser protoParserForTest = new ProtoParser(stencilClient, TestNestedRepeatedMessage.class.getCanonicalName());

        Properties propertiesForMessage = new Properties();
        propertiesForMessage.setProperty("3", "details");

        ProtoToFieldMapper protoToFieldMapperForTest = new ProtoToFieldMapper(protoParserForTest, propertiesForMessage);
        RedisMessageParser redisMessageParser = new RedisMessageParser(protoToFieldMapperForTest, protoParserForTest, redisSinkConfig);
        redisMessageParser.parse(esbMessage);
    }
}
