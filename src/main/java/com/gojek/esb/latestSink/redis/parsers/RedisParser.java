package com.gojek.esb.latestSink.redis.parsers;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.latestSink.redis.dataentry.RedisDataEntry;
import com.gojek.esb.metrics.Instrumentation;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.InvalidConfigurationException;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
public abstract class RedisParser {

    private ProtoParser protoParser;
    private RedisSinkConfig redisSinkConfig;
    private Instrumentation instrumentation;

    public abstract List<RedisDataEntry> parse(EsbMessage esbMessage);

    public List<RedisDataEntry> parse(List<EsbMessage> esbMessages) {
        return esbMessages
            .stream()
            .map(esbMessage -> this.parse(esbMessage))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    DynamicMessage parseEsbMessage(EsbMessage esbMessage) {
        DynamicMessage parsedMessage;
        try {
            parsedMessage = protoParser.parse(getPayload(esbMessage));
        } catch (InvalidProtocolBufferException e) {
            instrumentation.captureFatalError(e, "Unable to parse data when reading Key");
            throw new IllegalArgumentException(e);
        }
        return parsedMessage;
    }

    String parseTemplate(DynamicMessage data, String template) {
        if (StringUtils.isEmpty(template)) {
            IllegalArgumentException invalidTemplateException = new IllegalArgumentException("Invalid configuration, Collection key or key is null or empty");
            instrumentation.captureFatalError(invalidTemplateException, "Template {} is invalid", template);
            throw invalidTemplateException;
        }
        String[] templateStrings = template.split(",");
        if (templateStrings.length == 0) {
            String message = String.format("Empty key configuration: '%s'", template);
            InvalidConfigurationException emptyKeyException = new InvalidConfigurationException(message);
            instrumentation.captureFatalError(emptyKeyException, message);
            throw emptyKeyException;
        }
        templateStrings = Arrays
                .stream(templateStrings)
                .map(String::trim)
                .toArray(String[]::new);
        String templatePattern = templateStrings[0];
        String templateVariables = StringUtils.join(Arrays.copyOfRange(templateStrings, 1, templateStrings.length), ",");
        String renderedTemplate = renderStringTemplate(data, templatePattern, templateVariables);
        return StringUtils.isEmpty(templateVariables)
                ? templatePattern
                : renderedTemplate;
    }

    private String renderStringTemplate(DynamicMessage parsedMessage, String pattern, String patternVariables) {
        if (StringUtils.isEmpty(patternVariables)) {
            return pattern;
        }
        List<String> patternVariableFieldNumbers = Arrays.asList(patternVariables.split(","));
        Object[] patternVariableData = patternVariableFieldNumbers
                .stream()
                .map(fieldNumber -> getDataByFieldNumber(parsedMessage, fieldNumber))
                .toArray();
        return String.format(pattern, patternVariableData);
    }

    Object getDataByFieldNumber(DynamicMessage parsedMessage, String fieldNumber) {
        Integer fieldNumberInt = null;
        try {
            fieldNumberInt = Integer.valueOf(fieldNumber);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Proto Index");
        }
        Descriptors.FieldDescriptor fieldDescriptor = parsedMessage.getDescriptorForType().findFieldByNumber(fieldNumberInt);
        if (fieldDescriptor == null) {
            String message = String.format("Descriptor not found for index: %s", fieldNumber);
            IllegalArgumentException descriptorNotFoundException = new IllegalArgumentException(message);
            instrumentation.captureFatalError(descriptorNotFoundException, message);
            throw descriptorNotFoundException;
        }
        return parsedMessage.getField(fieldDescriptor);
    }

    byte[] getPayload(EsbMessage esbMessage) {
        if (redisSinkConfig.getKafkaRecordParserMode().equals("key")) {
            return esbMessage.getLogKey();
        } else {
            return esbMessage.getLogMessage();
        }
    }
}
