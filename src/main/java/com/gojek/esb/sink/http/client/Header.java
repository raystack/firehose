package com.gojek.esb.sink.http.client;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class Header {

    private java.util.Map<String, String> headerMap;

    public Header(String headerConfig) {
        headerMap = Arrays
                .stream(headerConfig.split(","))
                .filter(headerKeyValue -> !headerKeyValue.trim().isEmpty())
                .collect(Collectors.toMap(
                        headerKeyValue -> headerKeyValue.split(":")[0], headerKeyValue -> headerKeyValue.split(":")[1]));
    }

    public Map<String, String> getAll() {
        return headerMap;
    }

}
