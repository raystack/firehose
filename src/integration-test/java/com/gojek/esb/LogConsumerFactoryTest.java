package com.gojek.esb;

import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.factory.LogConsumerFactory;
import com.gojek.esb.sink.HttpSink;
import com.gojek.esb.sink.db.DBSink;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class LogConsumerFactoryTest {

    HashMap<String, String> config;

    @Before
    public void setup() {
        config = new HashMap<String, String>() {{
            put("KAFKA_TOPIC", "some topic");
            put("KAFKA_ADDRESS", "localhost:1111");
            put("LOGSINK_PROTO_SCHEMA", "com.gojek.esb.driverlocation.DriverLocationLogMessage");
        }};
    }

    @Test
    public void shouldCreateBatchLogConsumer() {
        config.put("STREAMING", "FALSE");
        config.put("SINK", "http");
        LogConsumerFactory logConsumerFactory = new LogConsumerFactory(config);

        Assert.assertEquals(logConsumerFactory.buildConsumer().getClass(), LogConsumer.class);
    }

    @Test
    public void shouldCreateHttpSinkConsumer() {
        config.put("STREAMING", "FALSE");
        config.put("SINK", "http");
        config.put("HTTPSINK_RETRY_STATUS_CODE_RANGES", "405-500,501-600");
        LogConsumerFactory logConsumerFactory = new LogConsumerFactory(config);

        Assert.assertEquals(logConsumerFactory.buildConsumer().getSink().getClass(), HttpSink.class);
    }

    @Test
    public void shouldCreateDBSinkConsumer() {
        config.put("STREAMING", "FALSE");
        config.put("SINK", "DB");
        config.putAll(dbConfig());
        LogConsumerFactory logConsumerFactory = new LogConsumerFactory(config);

        Assert.assertEquals(logConsumerFactory.buildConsumer().getSink().getClass(), DBSink.class);
    }

    private Map<String, String> dbConfig() {
        return new HashMap<String, String>() {{
            put("PROTO_SCHEMA", "com.gojek.esb.feedback.FeedbackLogMessage");
            put("JDBC_DRIVER", "org.postgresql.Driver");
            put("TABLE_NAME", "dfs.feedback_message");
            put("DB_USERNAME", "postgres");
            put("DB_PASSWORD", "");
            put("DB_URL", "jdbc:postgresql://127.0.0.1:5432/");

            put("PROTO_TO_COLUMN_MAPPING", "{\"1\":\"order_number\",\"2\":\"event_timestamp\",\"3\":\"driver_id\",\"4\":\"customer_id\",\"5\":\"feedback_rating\",\"6\":\"feedback_comment\"}");
            put("UNIQUE_KEYS", "order_number, event_timestamp");
            put("INITIAL_EXPIRY_TIME_IN_MS", "10");
            put("BACKOFF_RATE", "2");
            put("MAXIMUM_EXPIRY_TIME_IN_MS", "60000");
            put("MAXIMUM_CONNECTION_POOL_SIZE", "10");
            put("DB_CONNECTION_TIMEOUT_MS", "250");
            put("DB_IDLE_TIMEOUT_MS", "30000");
            put("DB_MINIMUM_IDLE", "0");
        }};
    }
}