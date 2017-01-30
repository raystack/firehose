package com.gojek.esb.consumer;

import com.gojek.esb.factory.LogConsumerFactory;
import com.gojek.esb.sink.DBSink;
import com.gojek.esb.sink.HttpSink;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;


public class LogConsumerFactoryTest {

    HashMap<String, String> config;

    @Before
    public void setup(){
        config = new HashMap<String, String>() {{
            put("KAFKA_TOPIC", "some topic");
            put("KAFKA_ADDRESS", "localhost:1111");

        }};
    }

    @Test
    public void shouldCreateBatchLogConsumer(){
        config.put("STREAMING", "FALSE");
        LogConsumerFactory logConsumerFactory = new LogConsumerFactory(config);

        assertEquals(logConsumerFactory.getConsumer().getClass(), LogConsumer.class);

    }

    @Test
    public void shouldCreateHttpSinkConsumer(){
        config.put("STREAMING", "FALSE");
        config.put("SINK", "HTTP");
        LogConsumerFactory logConsumerFactory = new LogConsumerFactory(config);

        assertEquals(logConsumerFactory.getConsumer().getSink().getClass(), HttpSink.class);
    }

    @Test
    public void shouldCreateDBSinkConsumer(){
        config.put("STREAMING", "FALSE");
        config.put("SINK", "DB");
        config.putAll(dbConfig());
        LogConsumerFactory logConsumerFactory = new LogConsumerFactory(config);


        assertEquals(logConsumerFactory.getConsumer().getSink().getClass(), DBSink.class);
    }

    private Map<String,String> dbConfig(){
        return new HashMap<String, String>(){{
            put("PROTO_SCHEMA","com.gojek.esb.feedback.FeedbackLogMessage");
        }};
    }
}