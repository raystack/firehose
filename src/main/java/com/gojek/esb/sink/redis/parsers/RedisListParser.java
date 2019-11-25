package com.gojek.esb.sink.redis.parsers;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import com.gojek.esb.sink.redis.dataentry.RedisListEntry;
import com.google.protobuf.DynamicMessage;

import java.util.ArrayList;
import java.util.List;

public class RedisListParser extends RedisParser {
    private ProtoParser protoParser;
    private RedisSinkConfig redisSinkConfig;

    public RedisListParser(ProtoParser protoParser, RedisSinkConfig redisSinkConfig) {
        super(protoParser, redisSinkConfig);
        this.protoParser = protoParser;
        this.redisSinkConfig = redisSinkConfig;
    }

    @Override
    public List<RedisDataEntry> parse(EsbMessage esbMessage) {
        DynamicMessage parsedMessage = parseEsbMessage(esbMessage);
        String redisKey = parseTemplate(parsedMessage, redisSinkConfig.getRedisKeyTemplate());
        List<RedisDataEntry> messageEntries = new ArrayList<>();
        String protoIndex = redisSinkConfig.getListDataProtoIndex();
        messageEntries.add(new RedisListEntry(redisKey, getDataByFieldNumber(parsedMessage, protoIndex).toString()));
        return messageEntries;
    }
}
