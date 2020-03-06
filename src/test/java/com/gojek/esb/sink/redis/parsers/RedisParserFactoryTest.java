package com.gojek.esb.sink.redis.parsers;

import com.gojek.de.stencil.client.ClassLoadStencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.config.enums.RedisSinkType;
import com.gojek.esb.consumer.TestMessage;
import com.gojek.esb.proto.ProtoToFieldMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Properties;

import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class RedisParserFactoryTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private RedisSinkConfig redisSinkConfig;
    private ClassLoadStencilClient stencilClient;
    private ProtoToFieldMapper protoToFieldMapper;
    private ProtoParser testMessageProtoParser;


    @Before
    public void setUp() throws Exception {
        stencilClient = new ClassLoadStencilClient();
        testMessageProtoParser = new ProtoParser(stencilClient, TestMessage.class.getCanonicalName());
        protoToFieldMapper = new ProtoToFieldMapper(testMessageProtoParser, getProperties("3", "details"));
    }

    private void setRedisSinkConfig(RedisSinkType redisSinkType) {
        when(redisSinkConfig.getRedisSinkType()).thenReturn(redisSinkType);
        when(redisSinkConfig.getRedisListDataProtoIndex()).thenReturn("1");
    }

    @Test
    public void shouldReturnNewRedisListParser() {
        setRedisSinkConfig(RedisSinkType.LIST);

        RedisParser parser = RedisParserFactory.getParser(protoToFieldMapper, testMessageProtoParser, redisSinkConfig);

        Assert.assertEquals(RedisListParser.class, parser.getClass());
    }

    @Test
    public void shouldReturnNewRedisHashSetParser() {
        setRedisSinkConfig(RedisSinkType.HASHSET);

        RedisParser parser = RedisParserFactory.getParser(protoToFieldMapper, testMessageProtoParser, redisSinkConfig);

        Assert.assertEquals(RedisHashSetParser.class, parser.getClass());
    }

    private Properties getProperties(String s, String order) {
        Properties propertiesForKey = new Properties();
        propertiesForKey.setProperty(s, order);
        return propertiesForKey;
    }
}
