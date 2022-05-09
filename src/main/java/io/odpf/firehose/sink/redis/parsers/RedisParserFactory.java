package io.odpf.firehose.sink.redis.parsers;

import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.firehose.config.RedisSinkConfig;
import io.odpf.firehose.proto.ProtoToFieldMapper;
import io.odpf.stencil.Parser;

/**
 * Redis parser factory.
 */
public class RedisParserFactory {

    /**
     * Gets parser.
     *
     * @param protoToFieldMapper the proto to field mapper
     * @param protoParser        the proto parser
     * @param redisSinkConfig    the redis sink config
     * @param statsDReporter     the statsd reporter
     * @return RedisParser
     */
    public static RedisParser getParser(ProtoToFieldMapper protoToFieldMapper, Parser protoParser, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {

        switch (redisSinkConfig.getSinkRedisDataType()) {
            case LIST:
                return new RedisListParser(protoParser, redisSinkConfig, statsDReporter);
            case HASHSET:
                return new RedisHashSetParser(protoToFieldMapper, protoParser, redisSinkConfig, statsDReporter);
            case KEYVALUE:
                return new RedisKeyValueParser(protoParser, redisSinkConfig, statsDReporter);
            default:
                return null;
        }
    }
}
