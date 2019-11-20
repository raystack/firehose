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

        String redisKey = getRedisKey(getPayload(esbMessage));
        Map<String, Object> protoToFieldMap = protoToFieldMapper.getFields(getPayload(esbMessage));
        List<RedisHashSetFieldEntry> messageEntries = new ArrayList<>();

        protoToFieldMap.forEach((s, o) -> {
            messageEntries.add(new RedisHashSetFieldEntry(redisKey, s, String.valueOf(o)));
        });

        return messageEntries;
    }

    private String getRedisKey(byte[] payload) {
        DynamicMessage parsedMessage;
        try {
            parsedMessage = protoParser.parse(payload);
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Unable to parse data when reading Key");
            throw new IllegalArgumentException(e);
        }
        String redisKeyPattern = redisSinkConfig.getRedisKeyPattern();
        String redisKeyVariables = redisSinkConfig.getRedisKeyVariables();
        if (StringUtils.isEmpty(redisKeyVariables)) {
            return redisKeyPattern;
        }
        List<String> redisKeyVariablesIndex = Arrays.asList(redisKeyVariables.split(","));
        Object[] redisKeyVariableData = redisKeyVariablesIndex
                .stream()
                .map(s -> {
                    Descriptors.FieldDescriptor fieldDescriptor = parsedMessage.getDescriptorForType().findFieldByNumber(Integer.valueOf(s));
                    if (fieldDescriptor == null) {
                        LOGGER.error(String.format("Descriptor not found for index: %s", s));
                        throw new IllegalArgumentException(String.format("Descriptor not found for index: %s", s));
                    }
                    return parsedMessage.getField(fieldDescriptor);
                }).toArray();
        return String.format(redisKeyPattern, redisKeyVariableData);
    }

    private byte[] getPayload(EsbMessage esbMessage) {
        if (redisSinkConfig.getKafkaRecordParserMode().equals("key")) {
            return esbMessage.getLogKey();
        } else {
            return esbMessage.getLogMessage();
        }
    }
}
