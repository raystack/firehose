package io.odpf.firehose.config;

import io.odpf.firehose.config.converter.ProtoIndexToFieldMapConverter;
import io.odpf.firehose.config.converter.RangeToHashMapConverter;
import org.aeonbits.owner.Config;

import java.util.Map;
import java.util.Properties;

public interface PrometheusSinkConfig extends AppConfig {

    @Key("SINK_PROM_RETRY_STATUS_CODE_RANGES")
    @DefaultValue("400-600")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkPromRetryStatusCodeRanges();

    @Key("SINK_PROM_REQUEST_LOG_STATUS_CODE_RANGES")
    @DefaultValue("400-499")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkPromRequestLogStatusCodeRanges();

    @Key("SINK_PROM_REQUEST_TIMEOUT_MS")
    @DefaultValue("10000")
    Integer getSinkPromRequestTimeoutMs();

    @Key("SINK_PROM_MAX_CONNECTIONS")
    @DefaultValue("10")
    Integer getSinkPromMaxConnections();

    @Key("SINK_PROM_SERVICE_URL")
    String getSinkPromServiceUrl();

    @Key("SINK_PROM_HEADERS")
    @DefaultValue("")
    String getSinkPromHeaders();

    @Config.Key("SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING")
    @Config.ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getSinkPromMetricNameProtoIndexMapping();

    @Config.Key("SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING")
    @Config.ConverterClass(ProtoIndexToFieldMapConverter.class)
    Properties getSinkPromLabelNameProtoIndexMapping();

    @Config.Key("SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX")
    Integer getSinkPromProtoEventTimestampIndex();

    @Config.Key("SINK_PROM_WITH_EVENT_TIMESTAMP")
    @DefaultValue("false")
    boolean isEventTimestampEnabled();
}
