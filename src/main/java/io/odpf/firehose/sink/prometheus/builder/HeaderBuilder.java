package io.odpf.firehose.sink.prometheus.builder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.odpf.firehose.sink.prometheus.PromSinkConstants.*;

public class HeaderBuilder {

    private final Map<String, String> baseHeaders;

    public HeaderBuilder(String headerConfig) {
        baseHeaders = Arrays.stream(headerConfig.split(","))
                        .filter(kv -> !kv.trim().isEmpty()).map(kv -> kv.split(":"))
                        .collect(Collectors.toMap(kv -> kv[0], kv -> kv[1]));
    }

    public Map<String, String> build() {
        Map<String, String> headers = new HashMap<>(baseHeaders);
        headers.put(CONTENT_ENCODING, CONTENT_ENCODING_DEFAULT);
        headers.put(PROMETHEUS_REMOTE_WRITE_VERSION, PROMETHEUS_REMOTE_WRITE_VERSION_DEFAULT);
        return headers;
    }
}
