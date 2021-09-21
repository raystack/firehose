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
import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.config.enums.FilterDataSourceType;
import io.odpf.firehose.config.enums.FilterMessageType;
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
import static io.odpf.firehose.config.enums.FilterMessageType.JSON;
import static io.odpf.firehose.config.enums.FilterMessageType.PROTOBUF;

/**
 * JSON-based filter to filter protobuf/JSON messages based on rules
 * defined in a JSON Schema string.
 */
public class JsonFilter implements Filter {

    private final ObjectMapper objectMapper;
    private final FilterDataSourceType filterDataSourceType;
    private final FilterMessageType messageType;
    private Method protoParser;
    private final String filterJsonSchema;
    private final Instrumentation instrumentation;
    private JsonSchema schema;
    private final JsonFormat.Printer jsonPrinter;
    private final String protoSchemaClass;

    /**
     * Instantiates a new Json filter.
     *
     * @param consumerConfig  the consumer config
     * @param instrumentation the instrumentation
     */
    public JsonFilter(KafkaConsumerConfig consumerConfig, Instrumentation instrumentation) {
        objectMapper = new ObjectMapper();
        JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        jsonPrinter = JsonFormat.printer().preservingProtoFieldNames();
        messageType = consumerConfig.getFilterMessageType();
        filterDataSourceType = consumerConfig.getFilterJsonDataSource();
        protoSchemaClass = consumerConfig.getFilterJsonSchemaProtoClass();
        filterJsonSchema = consumerConfig.getFilterJsonSchema();
        this.instrumentation = instrumentation;
        logConfigs();
        if (filterDataSourceType != NONE) {
            try {
                schema = schemaFactory.getSchema(filterJsonSchema);
            } catch (Exception e) {
                instrumentation.logError("Failed to parse JSON Schema " + e.getMessage());
            }
        }
        if (messageType == PROTOBUF) {
            try {
                protoParser = MethodUtils.getAccessibleMethod(Class.forName(protoSchemaClass), "parseFrom", byte[].class);
            } catch (ClassNotFoundException e) {
                instrumentation.logError("Failed to load Proto schema class " + e.getMessage());
            }
        }
    }

    private void logConfigs() {
        instrumentation.logInfo("\n\tFilter type: {}", filterDataSourceType);
        if (filterDataSourceType != NONE) {
            instrumentation.logInfo("\n\tFilter JSON Schema: {}", filterJsonSchema);
            instrumentation.logInfo("\n\tFilter message type: {}", messageType);
            if (messageType == PROTOBUF) {
                instrumentation.logInfo("\n\tMessage Proto class: {}", protoSchemaClass);
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
    public List<Message> filter(List<Message> messages) throws FilterException {
        if (filterDataSourceType == NONE) {
            return messages;
        }
        validateConfigs();
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


    private boolean evaluate(String jsonMessage) throws FilterException {
        JsonNode message;
        try {
            message = objectMapper.readTree(jsonMessage);
        } catch (JsonProcessingException e) {

            throw new FilterException("Failed to parse JSON message " + e.getMessage());
        }
        Set<ValidationMessage> validationErrors = schema.validate(message);
        validationErrors.forEach(error -> {
            instrumentation.logDebug("Message filtered out due to: ", error.getMessage());
        });
        return validationErrors.isEmpty();
    }

    private String deserialize(byte[] data) throws FilterException {
        String jsonMessage = "";
        if (messageType == PROTOBUF) {
            try {
                Object protoPojo = protoParser.invoke(null, data);
                jsonMessage = jsonPrinter.print((GeneratedMessageV3) protoPojo);

            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new FilterException("Failed to parse protobuf message", e);

            } catch (InvalidProtocolBufferException e) {
                throw new FilterException("Protobuf message is invalid ", e);
            }
        } else if (messageType == JSON) {
            jsonMessage = new String(data, Charset.defaultCharset());
        }
        return jsonMessage;
    }

    private void validateConfigs() throws FilterException {
        if (schema == null) {
            throw new FilterException("Filter JSON Schema is invalid");
        }
        if (messageType == null) {
            throw new FilterException("Filter ESB message type cannot be null");
        }
        if (messageType == PROTOBUF && protoParser == null) {
            throw new FilterException("Invalid Proto Schema class");
        }
    }
}
