package com.gojek.esb.sink.redis;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class RedisMessageParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisMessageParser.class);
    private ProtoToFieldMapper protoToFieldMapper;
    private ProtoParser protoParser;
    private RedisSinkConfig redisSinkConfig;

    public List<RedisHashSetFieldEntry> parse(EsbMessage esbMessage) {
        DynamicMessage parsedMessage = parseEsbMessage(esbMessage);
        String redisKey = parseTemplate(parsedMessage, redisSinkConfig.getRedisKeyTemplate());
        Map<String, Object> protoToFieldMap = protoToFieldMapper.getFields(getPayload(esbMessage));
        List<RedisHashSetFieldEntry> messageEntries = new ArrayList<>();
        protoToFieldMap.forEach((key, value) -> messageEntries.add(new RedisHashSetFieldEntry(redisKey, parseTemplate(parsedMessage, key), String.valueOf(value))));
        return messageEntries;
    }

    private DynamicMessage parseEsbMessage(EsbMessage esbMessage) {
        DynamicMessage parsedMessage;
        try {
            parsedMessage = protoParser.parse(getPayload(esbMessage));
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Unable to parse data when reading Key");
            throw new IllegalArgumentException(e);
        }
        return parsedMessage;
    }

    private String parseTemplate(DynamicMessage data, String template) {
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

    private Object getDataByFieldNumber(DynamicMessage parsedMessage, String fieldNumber) {
        Descriptors.FieldDescriptor fieldDescriptor = parsedMessage.getDescriptorForType().findFieldByNumber(Integer.valueOf(fieldNumber));
        if (fieldDescriptor == null) {
            LOGGER.error(String.format("Descriptor not found for index: %s", fieldNumber));
            throw new IllegalArgumentException(String.format("Descriptor not found for index: %s", fieldNumber));
        }
        return parsedMessage.getField(fieldDescriptor);
    }

    private byte[] getPayload(EsbMessage esbMessage) {
        if (redisSinkConfig.getKafkaRecordParserMode().equals("key")) {
            return esbMessage.getLogKey();
        } else {
            return esbMessage.getLogMessage();
        }
    }
}
