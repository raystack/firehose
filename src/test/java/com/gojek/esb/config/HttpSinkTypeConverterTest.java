package com.gojek.esb.config;

import com.gojek.esb.config.enums.HttpSinkType;
import com.gojek.esb.config.converter.HttpSinkTypeConverter;
import org.gradle.internal.impldep.org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HttpSinkTypeConverterTest {

    private HttpSinkTypeConverter httpSinkTypeConverter;

    @Before
    public void setUp() {
        httpSinkTypeConverter = new HttpSinkTypeConverter();
    }

    @Test
    public void shouldReturnJsonSinkTypeFromLowerCaseInput() {
        HttpSinkType httpSinkType = httpSinkTypeConverter.convert(null, "json");
        Assert.assertTrue(httpSinkType.equals(HttpSinkType.JSON));
    }

    @Test
    public void shouldReturnJsonSinkTypeFromUpperCaseInput() {
        HttpSinkType httpSinkType = httpSinkTypeConverter.convert(null, "JSON");
        Assert.assertTrue(httpSinkType.equals(HttpSinkType.JSON));
    }

    @Test
    public void shouldReturnJsonSinkTypeFromMixedCaseInput() {
        HttpSinkType httpSinkType = httpSinkTypeConverter.convert(null, "JsOn");
        Assert.assertTrue(httpSinkType.equals(HttpSinkType.JSON));
    }

    @Test
    public void shouldReturnProtoSinkTypeFromInput() {
        HttpSinkType httpSinkType = httpSinkTypeConverter.convert(null, "proto");
        Assert.assertTrue(httpSinkType.equals(HttpSinkType.PROTO));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnIllegalArgument() {
        httpSinkTypeConverter.convert(null, "");
    }
}
