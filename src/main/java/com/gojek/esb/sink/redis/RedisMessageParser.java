package com.gojek.esb.sink.redis;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class RedisMessageParser {

    private ProtoToFieldMapper protoToFieldMapper;
    private ProtoParser protoParser;
    private RedisSinkConfig redisSinkConfig;

    public List<RedisHashSetFieldEntry> parse(EsbMessage esbMessage) {

        String redisKey = getRedisKey(getPayload(esbMessage));
        Map<String, Object> protoToFieldMap = protoToFieldMapper.getFields(getPayload(esbMessage));
        List<RedisHashSetFieldEntry> messageEntries = new ArrayList<>();

        protoToFieldMap.forEach((s, o) -> {
            messageEntries.add(new RedisHashSetFieldEntry(redisKey, s, o.toString()));
        });

        return messageEntries;
    }

    private String getRedisKey(byte[] payload) {
        DynamicMessage parsedMessage;
        try {
            parsedMessage = protoParser.parse(payload);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(e);
        }
        Descriptors.FieldDescriptor redisKeyFieldDescriptor = parsedMessage.getDescriptorForType().findFieldByNumber(redisSinkConfig.getRedisKeyProtoIndex());
        return parsedMessage.getField(redisKeyFieldDescriptor).toString();
    }

    private byte[] getPayload(EsbMessage esbMessage) {
        if (redisSinkConfig.getKafkaRecordParserMode().equals("key")) {
            return esbMessage.getLogKey();
        } else {
            return esbMessage.getLogMessage();
        }
    }
}
