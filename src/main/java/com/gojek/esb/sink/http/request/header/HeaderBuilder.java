package com.gojek.esb.sink.http.request.header;

import com.gojek.esb.config.enums.HttpSinkParameterSourceType;
import com.gojek.esb.consumer.Message;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.google.common.base.CaseFormat;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class HeaderBuilder {

    private String headerConfig;
    private ProtoToFieldMapper protoToFieldMapper;
    private HttpSinkParameterSourceType httpSinkParameterSourceType;

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
                .collect(Collectors.toMap(e -> convertToCustomHeaders(e.getKey()), e -> e.getValue().toString()));
        baseHeaders.putAll(parameterizedHeaders);
        return baseHeaders;
    }

    public HeaderBuilder withParameterizedHeader(ProtoToFieldMapper protoToFieldmapper, HttpSinkParameterSourceType httpSinkParameterSource) {
        this.protoToFieldMapper = protoToFieldmapper;
        this.httpSinkParameterSourceType = httpSinkParameterSource;
        return this;
    }

    private String convertToCustomHeaders(String parameter) {
        String customHeader = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, parameter);
        customHeader = "X-" + customHeader;
        return customHeader;
    }
}
