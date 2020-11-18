package com.gojek.esb.sink.redis.parsers;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import com.gojek.esb.sink.redis.dataentry.RedisHashSetFieldEntry;
import com.google.protobuf.DynamicMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RedisHashSetParser extends RedisParser {
    private ProtoToFieldMapper protoToFieldMapper;
    private RedisSinkConfig redisSinkConfig;
    private StatsDReporter statsDReporter;

    public RedisHashSetParser(ProtoToFieldMapper protoToFieldMapper, ProtoParser protoParser, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        super(protoParser, redisSinkConfig);
        this.protoToFieldMapper = protoToFieldMapper;
        this.redisSinkConfig = redisSinkConfig;
        this.statsDReporter = statsDReporter;
    }

    @Override
    public List<RedisDataEntry> parse(EsbMessage esbMessage) {
        DynamicMessage parsedMessage = parseEsbMessage(esbMessage);
        String redisKey = parseTemplate(parsedMessage, redisSinkConfig.getRedisKeyTemplate());
        List<RedisDataEntry> messageEntries = new ArrayList<>();
        Map<String, Object> protoToFieldMap = protoToFieldMapper.getFields(getPayload(esbMessage));
        protoToFieldMap.forEach((key, value) -> messageEntries.add(new RedisHashSetFieldEntry(redisKey, parseTemplate(parsedMessage, key), String.valueOf(value), new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class))));
        return messageEntries;
    }
}
