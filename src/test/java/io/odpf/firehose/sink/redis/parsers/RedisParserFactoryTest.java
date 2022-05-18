package io.odpf.firehose.sink.redis.parsers;


import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.firehose.config.RedisSinkConfig;
import io.odpf.firehose.config.enums.RedisSinkDataType;
import io.odpf.firehose.consumer.TestMessage;
import io.odpf.firehose.proto.ProtoToFieldMapper;
import io.odpf.stencil.Parser;
import io.odpf.stencil.client.ClassLoadStencilClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class RedisParserFactoryTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private RedisSinkConfig redisSinkConfig;

    @Mock
    private StatsDReporter statsDReporter;

    private ClassLoadStencilClient stencilClient;
    private ProtoToFieldMapper protoToFieldMapper;
    private Parser testMessageProtoParser;


    @Before
    public void setUp() throws Exception {
        stencilClient = new ClassLoadStencilClient();
        testMessageProtoParser = stencilClient.getParser(TestMessage.class.getCanonicalName());
        protoToFieldMapper = new ProtoToFieldMapper(testMessageProtoParser, getProperties("3", "details"));
    }

    private void setRedisSinkConfig(RedisSinkDataType redisSinkDataType) {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(redisSinkDataType);
    }

    @Test
    public void shouldReturnNewRedisListParser() {
        setRedisSinkConfig(RedisSinkDataType.LIST);

        RedisParser parser = RedisParserFactory.getParser(protoToFieldMapper, testMessageProtoParser, redisSinkConfig, statsDReporter);

        assertEquals(RedisListParser.class, parser.getClass());
    }

    @Test
    public void shouldReturnNewRedisHashSetParser() {
        setRedisSinkConfig(RedisSinkDataType.HASHSET);

        RedisParser parser = RedisParserFactory.getParser(protoToFieldMapper, testMessageProtoParser, redisSinkConfig, statsDReporter);

        assertEquals(RedisHashSetParser.class, parser.getClass());
    }

    @Test
    public void shouldReturnNewRedisKeyValueParser() {
        setRedisSinkConfig(RedisSinkDataType.KEYVALUE);

        RedisParser parser = RedisParserFactory.getParser(protoToFieldMapper, testMessageProtoParser, redisSinkConfig, statsDReporter);

        assertEquals(RedisKeyValueParser.class, parser.getClass());
    }

    private Properties getProperties(String s, String order) {
        Properties propertiesForKey = new Properties();
        propertiesForKey.setProperty(s, order);
        return propertiesForKey;
    }
}
