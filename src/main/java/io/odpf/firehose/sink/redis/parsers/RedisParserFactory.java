package io.odpf.firehose.sink.redis.parsers;

import com.gojek.de.stencil.parser.ProtoParser;
import io.odpf.firehose.config.RedisSinkConfig;
import io.odpf.firehose.config.enums.RedisSinkDataType;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.proto.ProtoToFieldMapper;

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
