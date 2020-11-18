package com.gojek.esb.sink.redis.parsers;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import com.gojek.esb.sink.redis.dataentry.RedisListEntry;
import com.google.protobuf.DynamicMessage;

import java.util.ArrayList;
import java.util.List;

public class RedisListParser extends RedisParser {
    private RedisSinkConfig redisSinkConfig;
    private StatsDReporter statsDReporter;

    public RedisListParser(ProtoParser protoParser, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        super(protoParser, redisSinkConfig);
        this.redisSinkConfig = redisSinkConfig;
        this.statsDReporter = statsDReporter;
    }

    @Override
    public List<RedisDataEntry> parse(EsbMessage esbMessage) {
        DynamicMessage parsedMessage = parseEsbMessage(esbMessage);
        String redisKey = parseTemplate(parsedMessage, redisSinkConfig.getRedisKeyTemplate());
        String protoIndex = redisSinkConfig.getRedisListDataProtoIndex();
        if (protoIndex == null) {
            throw new IllegalArgumentException("Please provide REDIS_LIST_DATA_PROTO_INDEX in list sink");
        }
        List<RedisDataEntry> messageEntries = new ArrayList<>();
        messageEntries.add(new RedisListEntry(redisKey, getDataByFieldNumber(parsedMessage, protoIndex).toString(), new Instrumentation(statsDReporter, RedisListEntry.class)));
        return messageEntries;
    }
}
