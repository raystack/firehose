package io.odpf.firehose.sink;

import io.odpf.depot.message.SinkConnectorSchemaMessageMode;

import java.util.HashMap;
import java.util.Map;

public class SinkFactoryUtils {
    protected static Map<String, String> addAdditionalConfigsForSinkConnectors(Map<String, String> env) {
        Map<String, String> finalConfig = new HashMap<>(env);
        finalConfig.put("SINK_CONNECTOR_SCHEMA_MESSAGE_CLASS", env.getOrDefault("INPUT_SCHEMA_PROTO_CLASS", ""));
        finalConfig.put("SINK_CONNECTOR_SCHEMA_KEY_CLASS", env.getOrDefault("INPUT_SCHEMA_PROTO_CLASS", ""));
        //TODO discuss with folks if we should do env.get or appconfig.getConfigName
        finalConfig.put("SINK_CONNECTOR_SCHEMA_DATA_TYPE", env.getOrDefault("INPUT_DATA_FORMAT", "protobuf"));
        finalConfig.put("SINK_CONNECTOR_SCHEMA_JSON_DYNAMIC_SCHEMA_ENABLE", env.getOrDefault("INPUT_JSON_DYNAMIC_SCHEMA_ENABLE", "true"));
        finalConfig.put("SINK_CONNECTOR_SCHEMA_JSON_OUTPUT_DEFAULT_DATATYPE_STRING_ENABLE", env.getOrDefault("INPUT_JSON_OUTPUT_DEFAULT_DATATYPE_STRING_ENABLE", "true"));
        finalConfig.put("SINK_METRICS_APPLICATION_PREFIX", "firehose_");
        finalConfig.put("SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", env.getOrDefault("INPUT_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", "false"));
        finalConfig.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE",
                env.getOrDefault("KAFKA_RECORD_PARSER_MODE", "").equals("key") ? SinkConnectorSchemaMessageMode.LOG_KEY.name() : SinkConnectorSchemaMessageMode.LOG_MESSAGE.name());
        return finalConfig;
    }
}
