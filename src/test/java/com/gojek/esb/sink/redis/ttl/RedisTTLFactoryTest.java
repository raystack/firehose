package com.gojek.esb.sink.redis.ttl;

import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.config.enums.RedisTTLType;
import com.gojek.esb.exception.EglcConfigurationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class RedisTTLFactoryTest {

    @Mock
    private RedisSinkConfig redisSinkConfig;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        initMocks(this);
        when(redisSinkConfig.getRedisTTLType()).thenReturn(RedisTTLType.DISABLE);
    }

    @Test
    public void shouldReturnNoTTLIfNothingGiven() {
        RedisTTL redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        Assert.assertEquals(redisTTL.getClass(), NoRedisTTL.class);
    }

    @Test
    public void shouldReturnExactTimeTTL() {
        when(redisSinkConfig.getRedisTTLType()).thenReturn(RedisTTLType.EXACT_TIME);
        when(redisSinkConfig.getRedisTTLValue()).thenReturn(100L);
        RedisTTL redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        Assert.assertEquals(redisTTL.getClass(), ExactTimeTTL.class);
    }

    @Test
    public void shouldReturnDurationTTL() {
        when(redisSinkConfig.getRedisTTLType()).thenReturn(RedisTTLType.DURATION);
        when(redisSinkConfig.getRedisTTLValue()).thenReturn(100L);
        RedisTTL redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        Assert.assertEquals(redisTTL.getClass(), DurationTTL.class);
    }

    @Test
    public void shouldThrowExceptionInCaseOfInvalidConfiguration() {
        expectedException.expect(EglcConfigurationException.class);
        expectedException.expectMessage("Provide a positive TTL value");

        when(redisSinkConfig.getRedisTTLType()).thenReturn(RedisTTLType.DURATION);
        when(redisSinkConfig.getRedisTTLValue()).thenReturn(-1L);
        RedisTTLFactory.getTTl(redisSinkConfig);
    }
}
