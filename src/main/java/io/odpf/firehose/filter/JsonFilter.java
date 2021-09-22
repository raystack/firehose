package io.odpf.firehose.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import io.odpf.firehose.config.FilterConfig;
import io.odpf.firehose.config.enums.FilterDataSourceType;
import io.odpf.firehose.config.enums.FilterMessageFormatType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.odpf.firehose.config.enums.FilterDataSourceType.KEY;
import static io.odpf.firehose.config.enums.FilterDataSourceType.NONE;
import static io.odpf.firehose.config.enums.FilterMessageFormatType.PROTOBUF;

/**
 * JSON-based filter to filter protobuf/JSON messages based on rules
 * defined in a JSON Schema string.
 */
public class JsonFilter implements Filter {

    private final ObjectMapper objectMapper;
    private final FilterDataSourceType filterDataSourceType;
    private final FilterMessageFormatType messageType;
    private Method protoParser;
    private final Instrumentation instrumentation;
    private JsonSchema schema;
    private final JsonFormat.Printer jsonPrinter;

    /**
     * Instantiates a new Json filter.
     *
     * @param filterConfig    the consumer config
     * @param instrumentation the instrumentation
     */
    public JsonFilter(FilterConfig filterConfig, Instrumentation instrumentation) {
        objectMapper = new ObjectMapper();
        JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        jsonPrinter = JsonFormat.printer().preservingProtoFieldNames();
        messageType = filterConfig.getFilterMessageFormat();
        filterDataSourceType = filterConfig.getFilterJsonDataSource();
        String protoSchemaClass = filterConfig.getFilterJsonSchemaProtoClass();
        String filterJsonSchema = filterConfig.getFilterJsonSchema();
        this.instrumentation = instrumentation;
        if (filterDataSourceType != NONE) {
            try {
                schema = schemaFactory.getSchema(filterJsonSchema);
            } catch (Exception e) {
                throw new FilterException("Failed to parse JSON Schema " + e.getMessage());
            }
        }
        if (messageType == PROTOBUF) {
            try {
                protoParser = MethodUtils.getAccessibleMethod(Class.forName(protoSchemaClass), "parseFrom", byte[].class);
            } catch (ClassNotFoundException e) {
                throw new FilterException("Failed to load Proto schema class " + e.getMessage());
            }
        }
    }

    /**
     * Log configs.
     *
     * @param filterConfig    the filter config
     * @param instrumentation the instrumentation
     */
    public static void logConfigs(FilterConfig filterConfig, Instrumentation instrumentation) {
        instrumentation.logInfo("\n\tFilter type: {}", filterConfig.getFilterJsonDataSource());
        if (filterConfig.getFilterJsonDataSource() != NONE) {
            instrumentation.logInfo("\n\tFilter JSON Schema: {}", filterConfig.getFilterJsonSchema());
            instrumentation.logInfo("\n\tFilter message type: {}", filterConfig.getFilterMessageFormat());
            if (filterConfig.getFilterMessageFormat() == PROTOBUF) {
                instrumentation.logInfo("\n\tMessage Proto class: {}", filterConfig.getFilterJsonSchemaProtoClass());
            }
        } else {
            instrumentation.logInfo("No filter is selected");
        }
    }

    /**
     * method to filter the EsbMessages.
     *
     * @param messages the json/protobuf records in binary format that are wrapped in {@link Message}
     * @return the list of filtered Messages
     * @throws FilterException the filter exception
     */
    @Override
    public List<Message> filter(List<Message> messages) {
        if (filterDataSourceType == NONE) {
            return messages;
        }
        List<Message> filteredMessages = new ArrayList<>();
        for (Message message : messages) {
            byte[] data = (filterDataSourceType.equals(KEY)) ? message.getLogKey() : message.getLogMessage();
            String jsonMessage = deserialize(data);
            if (evaluate(jsonMessage)) {
                filteredMessages.add(message);
            }
        }
        return filteredMessages;
    }


    private boolean evaluate(String jsonMessage) {
        try {
            JsonNode message = objectMapper.readTree(jsonMessage);
            Set<ValidationMessage> validationErrors = schema.validate(message);
            validationErrors.forEach(error -> {
                instrumentation.logDebug("Message filtered out due to: ", error.getMessage());
            });
            return validationErrors.isEmpty();
        } catch (JsonProcessingException e) {
            throw new FilterException("Failed to parse JSON message " + e.getMessage());
        }
    }

    private String deserialize(byte[] data) {
        switch (messageType) {
            case PROTOBUF:
                try {
                    Object protoPojo = protoParser.invoke(null, data);
                    return jsonPrinter.print((GeneratedMessageV3) protoPojo);

                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new FilterException("Failed to parse protobuf message", e);

                } catch (InvalidProtocolBufferException e) {
                    throw new FilterException("Protobuf message is invalid ", e);
                }
            case JSON:
                return new String(data, Charset.defaultCharset());
            default:
                throw new FilterException("Invalid message format type");
        }
    }

    /**
     * Validate configs.
     *
     * @param filterConfig the filter config
     */
    public static void validateConfigs(FilterConfig filterConfig) {
        if (filterConfig.getFilterJsonSchema() == null) {
            throw new FilterException("Filter JSON Schema is invalid");
        }
        if (filterConfig.getFilterMessageFormat() == null) {
            throw new FilterException("Filter ESB message type cannot be null");
        }
        if (filterConfig.getFilterMessageFormat() == PROTOBUF && filterConfig.getFilterJsonSchemaProtoClass() == null) {
            throw new FilterException("Proto Schema class cannot be null");
        }
    }
}
