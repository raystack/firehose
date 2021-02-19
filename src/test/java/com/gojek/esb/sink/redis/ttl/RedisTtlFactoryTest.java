package com.gojek.esb.sink.redis.ttl;

import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.config.enums.RedisSinkTtlType;
import com.gojek.esb.exception.EglcConfigurationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class RedisTtlFactoryTest {

    @Mock
    private RedisSinkConfig redisSinkConfig;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        initMocks(this);
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DISABLE);
    }

    @Test
    public void shouldReturnNoTTLIfNothingGiven() {
        RedisTtl redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        Assert.assertEquals(redisTTL.getClass(), NoRedisTtl.class);
    }

    @Test
    public void shouldReturnExactTimeTTL() {
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.EXACT_TIME);
        when(redisSinkConfig.getSinkRedisTtlValue()).thenReturn(100L);
        RedisTtl redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        Assert.assertEquals(redisTTL.getClass(), ExactTimeTtl.class);
    }

    @Test
    public void shouldReturnDurationTTL() {
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisTtlValue()).thenReturn(100L);
        RedisTtl redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        Assert.assertEquals(redisTTL.getClass(), DurationTtl.class);
    }

    @Test
    public void shouldThrowExceptionInCaseOfInvalidConfiguration() {
        expectedException.expect(EglcConfigurationException.class);
        expectedException.expectMessage("Provide a positive TTL value");

        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisTtlValue()).thenReturn(-1L);
        RedisTTLFactory.getTTl(redisSinkConfig);
    }
}
