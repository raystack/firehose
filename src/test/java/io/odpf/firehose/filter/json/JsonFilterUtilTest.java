package io.odpf.firehose.filter.json;

import io.odpf.firehose.config.FilterConfig;
import io.odpf.firehose.config.enums.FilterDataSourceType;
import io.odpf.firehose.consumer.TestMessage;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static io.odpf.firehose.config.enums.FilterMessageFormatType.PROTOBUF;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class JsonFilterUtilTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    private FilterConfig filterConfig;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldLogFilterConfigsForValidConfiguration() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        JsonFilterUtil.logConfigs(filterConfig, firehoseInstrumentation);
        verify(firehoseInstrumentation, times(1)).logInfo("\n\tFilter data source type: {}", FilterDataSourceType.MESSAGE);
        verify(firehoseInstrumentation, times(1)).logInfo("\n\tMessage Proto class: {}", TestMessage.class.getName());
        verify(firehoseInstrumentation, times(1)).logInfo("\n\tFilter JSON Schema: {}", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        verify(firehoseInstrumentation, times(1)).logInfo("\n\tFilter ESB message format: {}", PROTOBUF);
    }

    @Test
    public void shouldLogFilterConfigsForInvalidConfiguration() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        JsonFilterUtil.logConfigs(filterConfig, firehoseInstrumentation);
        verify(firehoseInstrumentation, times(1)).logInfo("\n\tFilter data source type: {}", FilterDataSourceType.MESSAGE);
        verify(firehoseInstrumentation, times(1)).logInfo("\n\tFilter JSON Schema: {}", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        verify(firehoseInstrumentation, times(1)).logInfo("\n\tFilter ESB message format: {}", (Object) null);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForNullJsonSchema() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        thrown.expect(IllegalArgumentException.class);
        JsonFilterUtil.validateConfigs(filterConfig, firehoseInstrumentation);
        verify(firehoseInstrumentation, times(1)).logError("Failed to create filter due to invalid config");
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForNullMessageFormat() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        thrown.expect(IllegalArgumentException.class);
        JsonFilterUtil.validateConfigs(filterConfig, firehoseInstrumentation);
        verify(firehoseInstrumentation, times(1)).logError("Failed to create filter due to invalid config");
    }

    @Test
    public void shouldThrowExceptionForNullProtoSchemaClassForProtobufMessageFormat() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        thrown.expect(IllegalArgumentException.class);
        JsonFilterUtil.validateConfigs(filterConfig, firehoseInstrumentation);
        verify(firehoseInstrumentation, times(1)).logError("Failed to create filter due to invalid config");
    }

    @Test
    public void shouldNotThrowIllegalArgumentExceptionForValidFilterConfig() {
        Map<String, String> filterConfigs = new HashMap<>();
        filterConfigs.put("FILTER_DATA_SOURCE", "message");
        filterConfigs.put("FILTER_ESB_MESSAGE_FORMAT", "PROTOBUF");
        filterConfigs.put("FILTER_JSON_SCHEMA", "{\"properties\":{\"order_number\":{\"const\":\"123\"}}}");
        filterConfigs.put("FILTER_SCHEMA_PROTO_CLASS", TestMessage.class.getName());
        filterConfig = ConfigFactory.create(FilterConfig.class, filterConfigs);
        JsonFilterUtil.validateConfigs(filterConfig, firehoseInstrumentation);
    }
}
