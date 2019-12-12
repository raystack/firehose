package com.gojek.esb.sink.redis.client;

import com.gojek.esb.sink.redis.dataentry.RedisDataEntry;
import lombok.AllArgsConstructor;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.List;

/**
 * Wraps Jedis client for only pipelined execute.
 */
@AllArgsConstructor
public class RedisClient {
    private Jedis jedis;

    public void execute(List<RedisDataEntry> entriesList) throws NoResponseException {
        try {
            Pipeline jedisPipelined = jedis.pipelined();
            jedisPipelined.multi();

            entriesList.forEach(redisDataEntry -> {
                redisDataEntry.pushMessage(jedisPipelined);
            });

            Response<List<Object>> responses = jedisPipelined.exec();
            jedisPipelined.sync();
            if (responses.get() == null || responses.get().isEmpty()) {
                throw new NoResponseException();
            }
        } finally {
            close();
        }
    }

    public void close() {
        jedis.close();
    }
}
