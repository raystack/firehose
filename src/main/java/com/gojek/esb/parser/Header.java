package com.gojek.esb.parser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class to parse the header.
 */
public class Header {

    public static Map<String, String> parse(String headers) {
        return Arrays.stream(headers.split(",")).filter(headerKeyValue -> !headerKeyValue.trim().isEmpty()).collect(Collectors.toMap(
                headerKeyValue -> headerKeyValue.split(":")[0], headerKeyValue -> headerKeyValue.split(":")[1]));
    }
}
