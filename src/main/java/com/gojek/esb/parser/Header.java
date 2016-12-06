package com.gojek.esb.parser;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to parse the header.
 */
public class Header {

    public static Map<String, String> parse(String headers) {
        HashMap<String, String> map = new HashMap<>();

        String[] headerStrings = headers.split(",");
        for (String string : headerStrings) {
            if (!string.trim().isEmpty()) {
                String[] keyValue = string.trim().split(":");
                map.put(keyValue[0], keyValue[1]);
            }
        }

        return map;
    }
}
