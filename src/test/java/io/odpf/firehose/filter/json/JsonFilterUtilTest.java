package io.odpf.firehose.filter.json;

import io.odpf.firehose.config.FilterConfig;
import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.config.enums.FilterDataSourceType;
import io.odpf.firehose.consumer.TestMessage;
import io.odpf.firehose.metrics.Instrumentation;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class JsonFilterUtilTest {

    @Mock
    private Instrumentation instrumentation;

    private FilterConfig filterConfig;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldLogFilterTypeIfFilterTypeIsNone() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_JSON_DATA_SOURCE", "none");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        JsonFilterUtil.logConfigs(filterConfig, instrumentation);
        verify(instrumentation, times(1)).logInfo("No filter is selected");
    }

    @Test
    public void shouldLogFilterTypeIfFilterDataSourceIsNotNoneForProtobufMessageType() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_JSON_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_JSON_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        JsonFilterUtil.logConfigs(filterConfig, instrumentation);
        verify(instrumentation, times(1)).logInfo("\n\tFilter type: {}", FilterDataSourceType.MESSAGE);
        verify(instrumentation, times(1)).logInfo("\n\tMessage Proto class: {}", TestMessage.class.getName());
        verify(instrumentation, times(1)).logInfo("\n\tFilter JSON Schema: {}", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
    }
}
