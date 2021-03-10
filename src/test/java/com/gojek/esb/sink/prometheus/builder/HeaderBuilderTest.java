package com.gojek.esb.sink.prometheus.builder;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class HeaderBuilderTest {

    @Test
    public void shouldGenerateBaseHeader() {
        String headerConfig = "content-type:json";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);

        assertEquals("json", headerBuilder.build().get("content-type"));
    }

    @Test
    public void shouldHandleMultipleHeader() {
        String headerConfig = "Authorization:auth_token,Accept:text/plain";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);

        Map<String, String> header = headerBuilder.build();
        assertEquals("auth_token", header.get("Authorization"));
        assertEquals("text/plain", header.get("Accept"));
    }

    @Test
    public void shouldParseWithRequiredPromHeadersInBetween() {
        String headerConfig = "foo:bar,,accept:text/plain";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);
        Map<String, String> expected = new HashMap<String, String>() {
            {
                put("foo", "bar");
                put("accept", "text/plain");
                put("X-Prometheus-Remote-Write-Version", "0.1.0");
                put("Content-Encoding", "snappy");
            }
        };
        assertEquals(expected, headerBuilder.build());
    }

    @Test
    public void shouldNotThrowNullPointerExceptionWhenHeaderConfigEmpty() {
        String headerConfig = "";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);

        headerBuilder.build();
    }

    @Test
    public void shouldAddBaseHeaderPerMessageIfNotParameterized() {
        String headerConfig = "content-type:json";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);

        Map<String, String> header = headerBuilder.build();
        assertEquals("json", header.get("content-type"));
    }
}

