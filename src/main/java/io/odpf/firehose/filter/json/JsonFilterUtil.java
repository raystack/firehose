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
        instrumentation.logInfo("\n\tFilter message type: {}", filterConfig.getFilterMessageFormat());
        if (filterConfig.getFilterMessageFormat() == PROTOBUF) {
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
        try {
            if (filterConfig.getFilterJsonSchema() == null) {
                throw new IllegalArgumentException("Filter JSON Schema is invalid");
            }
            if (filterConfig.getFilterMessageFormat() == null) {
                throw new IllegalArgumentException("Filter ESB message type cannot be null");
            }
            if (filterConfig.getFilterMessageFormat() == PROTOBUF && filterConfig.getFilterSchemaProtoClass() == null) {
                throw new IllegalArgumentException("Proto Schema class cannot be null");
            }
        } finally {
            instrumentation.logError("Failed to create filter due to invalid config");
        }
    }
}
