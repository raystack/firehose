package com.gojek.esb.sink.redis.parsers;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.RedisSinkConfig;
import com.gojek.esb.config.enums.RedisSinkDataType;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.proto.ProtoToFieldMapper;

public class RedisParserFactory {

    public static RedisParser getParser(ProtoToFieldMapper protoToFieldMapper, ProtoParser protoParser, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {

        RedisParser redisParser;

        if (redisSinkConfig.getSinkRedisDataType().equals(RedisSinkDataType.LIST)) {
            redisParser = new RedisListParser(protoParser, redisSinkConfig, statsDReporter);
        } else {
            redisParser = new RedisHashSetParser(protoToFieldMapper, protoParser, redisSinkConfig, statsDReporter);
        }
        return redisParser;
    }
}
