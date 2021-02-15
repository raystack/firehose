package com.gojek.esb.config;

import com.gojek.esb.config.converter.RedisSinkDataTypeConverter;
import com.gojek.esb.config.enums.RedisSinkDataType;
import org.gradle.internal.impldep.org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RedisSinkDataTypeConverterTest {

    private RedisSinkDataTypeConverter redisSinkDataTypeConverter;

    @Before
    public void setUp() {
        redisSinkDataTypeConverter = new RedisSinkDataTypeConverter();
    }

    @Test
    public void shouldReturnListSinkTypeFromLowerCaseInput() {
        RedisSinkDataType redisSinkDataType = redisSinkDataTypeConverter.convert(null, "list");
        Assert.assertTrue(redisSinkDataType.equals(RedisSinkDataType.LIST));
    }

    @Test
    public void shouldReturnListSinkTypeFromUpperCaseInput() {
        RedisSinkDataType redisSinkDataType = redisSinkDataTypeConverter.convert(null, "LIST");
        Assert.assertTrue(redisSinkDataType.equals(RedisSinkDataType.LIST));
    }

    @Test
    public void shouldReturnListSinkTypeFromMixedCaseInput() {
        RedisSinkDataType redisSinkDataType = redisSinkDataTypeConverter.convert(null, "LiSt");
        Assert.assertTrue(redisSinkDataType.equals(RedisSinkDataType.LIST));
    }

    @Test
    public void shouldReturnHashSetSinkTypeFromInput() {
        RedisSinkDataType redisSinkDataType = redisSinkDataTypeConverter.convert(null, "hashset");
        Assert.assertTrue(redisSinkDataType.equals(RedisSinkDataType.HASHSET));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnIllegalArgument() {
        redisSinkDataTypeConverter.convert(null, "");
    }

}
