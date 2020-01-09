package com.gojek.esb.sink.redis.parsers;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import com.gojek.esb.sink.redis.dataentry.RedisListEntry;
import com.gojek.esb.metrics.Instrumentation;
import com.google.protobuf.DynamicMessage;

import java.util.ArrayList;
import java.util.List;

public class RedisListParser extends RedisParser {
    private RedisSinkConfig redisSinkConfig;
    private Instrumentation instrumentation;

    public RedisListParser(ProtoParser protoParser, RedisSinkConfig redisSinkConfig, Instrumentation instrumentation) {
        super(protoParser, redisSinkConfig, instrumentation);
        this.redisSinkConfig = redisSinkConfig;
        this.instrumentation = instrumentation;
    }

    @Override
    public List<RedisDataEntry> parse(EsbMessage esbMessage) {
        DynamicMessage parsedMessage = parseEsbMessage(esbMessage);
        String redisKey = parseTemplate(parsedMessage, redisSinkConfig.getRedisKeyTemplate());
        String protoIndex = redisSinkConfig.getRedisListDataProtoIndex();
        if (protoIndex == null) {
            IllegalArgumentException illegalArgumentException = new IllegalArgumentException("Please provide REDIS_LIST_DATA_PROTO_INDEX in list sink");
            instrumentation.captureFatalError(illegalArgumentException);
            throw illegalArgumentException;
        }
        List<RedisDataEntry> messageEntries = new ArrayList<>();
        messageEntries.add(new RedisListEntry(redisKey, getDataByFieldNumber(parsedMessage, protoIndex).toString()));
        return messageEntries;
    }
}
