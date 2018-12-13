package com.gojek.esb.sink.http.client.deserializer;

import com.gojek.esb.consumer.EsbMessage;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.core.IsEqual.equalTo;

public class JsonWrapperDeserializerTest {

    private EsbMessage esbMessage;

    @Test
    public void canDeserializeListOfEsbMessagesIntoJsonContainingBinary() {
        JsonWrapperDeserializer deserializer = new JsonWrapperDeserializer();
        esbMessage = new EsbMessage(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100);
        List<String> actualOutput = deserializer.deserialize(Collections.singletonList(esbMessage));
        Assert.assertThat(actualOutput, equalTo(Collections.singletonList("{\"topic\":\"sample-topic\",\"log_key\":\"ChQ\\u003d\",\"log_message\":\"AQI\\u003d\"}")));
    }
}
