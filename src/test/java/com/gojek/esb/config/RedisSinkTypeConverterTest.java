package com.gojek.esb.config;

import com.gojek.esb.config.converter.RedisSinkTypeConverter;
import com.gojek.esb.config.enums.RedisSinkType;
import org.gradle.internal.impldep.org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RedisSinkTypeConverterTest {

    private RedisSinkTypeConverter redisSinkTypeConverter;

    @Before
    public void setUp() {
        redisSinkTypeConverter = new RedisSinkTypeConverter();
    }

    @Test
    public void shouldReturnListSinkTypeFromLowerCaseInput() {
        RedisSinkType redisSinkType = redisSinkTypeConverter.convert(null, "list");
        Assert.assertTrue(redisSinkType.equals(RedisSinkType.LIST));
    }

    @Test
    public void shouldReturnListSinkTypeFromUpperCaseInput() {
        RedisSinkType redisSinkType = redisSinkTypeConverter.convert(null, "LIST");
        Assert.assertTrue(redisSinkType.equals(RedisSinkType.LIST));
    }

    @Test
    public void shouldReturnListSinkTypeFromMixedCaseInput() {
        RedisSinkType redisSinkType = redisSinkTypeConverter.convert(null, "LiSt");
        Assert.assertTrue(redisSinkType.equals(RedisSinkType.LIST));
    }

    @Test
    public void shouldReturnHashSetSinkTypeFromInput() {
        RedisSinkType redisSinkType = redisSinkTypeConverter.convert(null, "hashset");
        Assert.assertTrue(redisSinkType.equals(RedisSinkType.HASHSET));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnIllegalArgument() {
        redisSinkTypeConverter.convert(null, "");
    }

}
