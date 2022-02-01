package io.odpf.firehose.sink.redis.parsers;

import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.config.RedisSinkConfig;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.redis.dataentry.RedisDataEntry;
import io.odpf.firehose.sink.redis.dataentry.RedisKeyValueEntry;
import io.odpf.stencil.Parser;

import java.util.Collections;
import java.util.List;

public class RedisKeyValueParser extends RedisParser {
    private RedisSinkConfig redisSinkConfig;
    private StatsDReporter statsDReporter;

    public RedisKeyValueParser(Parser protoParser, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        super(protoParser, redisSinkConfig);
        this.redisSinkConfig = redisSinkConfig;
        this.statsDReporter = statsDReporter;
    }

    @Override
    public List<RedisDataEntry> parse(Message message) {
        DynamicMessage parsedMessage = parseEsbMessage(message);
        String redisKey = parseTemplate(parsedMessage, redisSinkConfig.getSinkRedisKeyTemplate());
        String protoIndex = redisSinkConfig.getSinkRedisKeyValuetDataProtoIndex();
        if (protoIndex == null) {
            throw new IllegalArgumentException("Please provide SINK_REDIS_KEY_VALUE_DATA_PROTO_INDEX in key value sink");
        }
        Instrumentation instrumentation = new Instrumentation(statsDReporter, RedisKeyValueEntry.class);
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(redisKey, getDataByFieldNumber(parsedMessage, protoIndex).toString(), instrumentation);
        return Collections.singletonList(redisKeyValueEntry);
    }
}
