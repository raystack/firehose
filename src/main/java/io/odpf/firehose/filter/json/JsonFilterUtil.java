package io.odpf.firehose.filter.json;

import io.odpf.firehose.config.FilterConfig;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.experimental.UtilityClass;

import static io.odpf.firehose.config.enums.FilterMessageFormatType.PROTOBUF;

/**
 * The type Json filter util.
 */
@UtilityClass
public class JsonFilterUtil {

    /**
     * Log configs.
     *
     * @param filterConfig    the filter config
     * @param instrumentation the instrumentation
     */
    public static void logConfigs(FilterConfig filterConfig, Instrumentation instrumentation) {
        instrumentation.logInfo("\n\tFilter data source type: {}", filterConfig.getFilterDataSource());
        instrumentation.logInfo("\n\tFilter JSON Schema: {}", filterConfig.getFilterJsonSchema());
        instrumentation.logInfo("\n\tFilter ESB message format: {}", filterConfig.getFilterESBMessageFormat());
        if (filterConfig.getFilterESBMessageFormat() == PROTOBUF) {
            instrumentation.logInfo("\n\tMessage Proto class: {}", filterConfig.getFilterSchemaProtoClass());
        }
    }

    /**
     * Validate configs.
     *
     * @param filterConfig    the filter config
     * @param instrumentation the instrumentation
     */
    public static void validateConfigs(FilterConfig filterConfig, Instrumentation instrumentation) {
        if (filterConfig.getFilterJsonSchema() == null) {
            instrumentation.logError("Failed to create filter due to invalid config");
            throw new IllegalArgumentException("Filter JSON Schema is invalid");
        }
        if (filterConfig.getFilterESBMessageFormat() == null) {
            instrumentation.logError("Failed to create filter due to invalid config");
            throw new IllegalArgumentException("Filter ESB message type cannot be null");
        }
        if (filterConfig.getFilterESBMessageFormat() == PROTOBUF && filterConfig.getFilterSchemaProtoClass() == null) {
            instrumentation.logError("Failed to create filter due to invalid config");
            throw new IllegalArgumentException("Proto Schema class cannot be null");
        }
    }
}
