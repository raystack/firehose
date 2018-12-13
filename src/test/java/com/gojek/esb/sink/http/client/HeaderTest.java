package com.gojek.esb.sink.http.client;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class HeaderTest {

    @Test
    public void shouldParseHeaders() {
        String header = "foo:bar,accept:text/plain";
        Map<String, String> expectedHeader = new HashMap<String, String>() {{
            put("foo", "bar");
            put("accept", "text/plain");
        }};

        Map<String, String> actualHeader = new Header(header).getAll();

        assertEquals(expectedHeader, actualHeader);
    }

    @Test
    public void shouldParseEmptyHeaders() {
        String header = "";
        Map<String, String> expectedHeader = new HashMap<>();

        Map<String, String> actualHeader = new Header(header).getAll();

        assertEquals(expectedHeader, actualHeader);
    }

    @Test
    public void shouldParseWithNilHeadersInBetween() {

        String header = "foo:bar,,accept:text/plain";
        Map<String, String> expectedHeader = new HashMap<String, String>() {{
            put("foo", "bar");
            put("accept", "text/plain");
        }};

        Map<String, String> actualHeader = new Header(header).getAll();
        assertEquals(expectedHeader, actualHeader);
    }
}
