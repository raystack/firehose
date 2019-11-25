package com.gojek.esb.sink.redis.parsers;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

@AllArgsConstructor
public abstract class RedisParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(com.gojek.esb.sink.redis.parsers.RedisParser.class);
    private ProtoParser protoParser;
    private RedisSinkConfig redisSinkConfig;

    public abstract List<RedisDataEntry> parse(EsbMessage esbMessage);

    DynamicMessage parseEsbMessage(EsbMessage esbMessage) {
        DynamicMessage parsedMessage;
        try {
            parsedMessage = protoParser.parse(getPayload(esbMessage));
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Unable to parse data when reading Key");
            throw new IllegalArgumentException(e);
        }
        return parsedMessage;
    }

    String parseTemplate(DynamicMessage data, String template) {
        if (StringUtils.isEmpty(template)) {
            LOGGER.error(String.format("Template '%s' is invalid", template));
            throw new IllegalArgumentException("Invalid configuration, Collection key or key is null or empty");
        }
        String[] templateStrings = template.split(",");
        if (templateStrings.length == 0) {
            LOGGER.error(String.format("Empty key configuration: '%s'", template));
            throw new InvalidConfigurationException(String.format("Empty key configuration: '%s'", template));
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
        Descriptors.FieldDescriptor fieldDescriptor = parsedMessage.getDescriptorForType().findFieldByNumber(Integer.valueOf(fieldNumber));
        if (fieldDescriptor == null) {
            LOGGER.error(String.format("Descriptor not found for index: %s", fieldNumber));
            throw new IllegalArgumentException(String.format("Descriptor not found for index: %s", fieldNumber));
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
