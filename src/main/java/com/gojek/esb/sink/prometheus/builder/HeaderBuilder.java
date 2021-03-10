package com.gojek.esb.sink.prometheus.builder;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class HeaderBuilder {

    private String headerConfig;

    public HeaderBuilder(String headerConfig) {
        this.headerConfig = headerConfig;
    }

    private Map<String, String> buildConfigHeader() {
        return Arrays.stream(headerConfig.split(","))
                .filter(headerKeyValue -> !headerKeyValue.trim().isEmpty()).collect(Collectors
                        .toMap(headerKeyValue -> headerKeyValue.split(":")[0], headerKeyValue -> headerKeyValue.split(":")[1]));
    }

    public Map<String, String> build() {
        Map<String, String> baseHeaders = buildConfigHeader();
        baseHeaders.put("Content-Encoding", "snappy");
        baseHeaders.put("X-Prometheus-Remote-Write-Version", "0.1.0");
        return baseHeaders;
    }
}
