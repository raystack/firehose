package io.odpf.firehose.sink.http.request.header;

import io.odpf.firehose.config.enums.HttpSinkParameterSourceType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.proto.ProtoToFieldMapper;
import com.google.common.base.CaseFormat;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Header builder for http requests.
 */
public class HeaderBuilder {

    private String headerConfig;
    private ProtoToFieldMapper protoToFieldMapper;
    private HttpSinkParameterSourceType httpSinkParameterSourceType;

    /**
     * Instantiates a new Header builder.
     *
     * @param headerConfig the header config
     */
    public HeaderBuilder(String headerConfig) {
        this.headerConfig = headerConfig;
    }

    public Map<String, String> build() {
        return Arrays.stream(headerConfig.split(","))
                .filter(headerKeyValue -> !headerKeyValue.trim().isEmpty()).collect(Collectors
                        .toMap(headerKeyValue -> headerKeyValue.split(":")[0], headerKeyValue -> headerKeyValue.split(":")[1]));
    }

    public Map<String, String> build(Message message) {
        Map<String, String> baseHeaders = build();
        if (protoToFieldMapper == null) {
            return baseHeaders;
        }

        // flow for parameterized headers
        Map<String, Object> paramMap = protoToFieldMapper
                .getFields((httpSinkParameterSourceType == HttpSinkParameterSourceType.KEY) ? message.getLogKey()
                        : message.getLogMessage());

        Map<String, String> parameterizedHeaders = paramMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
        baseHeaders.putAll(parameterizedHeaders);
        return baseHeaders;
    }

    public HeaderBuilder withParameterizedHeader(ProtoToFieldMapper protoToFieldmapper, HttpSinkParameterSourceType httpSinkParameterSource) {
        this.protoToFieldMapper = protoToFieldmapper;
        this.httpSinkParameterSourceType = httpSinkParameterSource;
        return this;
    }

    private String convertToCustomHeaders(String parameter) {
        return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, parameter);
    }
}
