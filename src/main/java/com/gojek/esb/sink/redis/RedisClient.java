package com.gojek.esb.sink.redis;

import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.List;

/**
 * Wraps Jedis client for only pipelined execute.
 */
@AllArgsConstructor
public class RedisClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisClient.class);
    private Jedis jedis;

    public void execute(List<RedisHashSetFieldEntry> entriesList) {
        try {
            Pipeline jedisPipelined = jedis.pipelined();
            jedisPipelined.multi();
            entriesList.forEach(redisHashSetFieldEntry -> {
                LOGGER.debug("Pushing {}, {}, {} to redis.", redisHashSetFieldEntry.getKey(), redisHashSetFieldEntry.getField(), redisHashSetFieldEntry.getValue());
                jedisPipelined.hset(
                        redisHashSetFieldEntry.getKey(),
                        redisHashSetFieldEntry.getField(),
                        redisHashSetFieldEntry.getValue()
                );
            });
            Response<List<Object>> responses = jedisPipelined.exec();
            jedisPipelined.sync();
            if (responses.get() == null || responses.get().isEmpty()) {
                LOGGER.error("Redis Pipeline error: no responds received");
                throw new RuntimeException("Redis Pipeline error: no responds received");
            }
        } finally {
            close();
        }
    }

    public void close() {
        jedis.close();
    }
}
