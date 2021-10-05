package io.odpf.firehose.filter.json;

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
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.filter.Filter;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.metrics.Instrumentation;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.odpf.firehose.config.enums.FilterDataSourceType.KEY;

/**
 * JSON-based filter to filter protobuf/JSON messages based on rules
 * defined in a JSON Schema string.
 */
public class JsonFilter implements Filter {

    private final FilterConfig filterConfig;
    private final Instrumentation instrumentation;
    private final JsonSchema schema;
    private final JsonFormat.Printer jsonPrinter = JsonFormat.printer().preservingProtoFieldNames();
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Instantiates a new Json filter.
     *
     * @param filterConfig    the consumer config
     * @param instrumentation the instrumentation
     */
    public JsonFilter(FilterConfig filterConfig, Instrumentation instrumentation) {
        this.instrumentation = instrumentation;
        this.filterConfig = filterConfig;
        JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        schema = schemaFactory.getSchema(filterConfig.getFilterJsonSchema());
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
        List<Message> filteredMessages = new ArrayList<>();
        for (Message message : messages) {
            byte[] data = (filterConfig.getFilterDataSource().equals(KEY)) ? message.getLogKey() : message.getLogMessage();
            String jsonMessage = deserialize(data);
            if (evaluate(jsonMessage)) {
                filteredMessages.add(message);
            }
        }
        return filteredMessages;
    }

    private boolean evaluate(String jsonMessage) throws FilterException {
        try {
            JsonNode message = objectMapper.readTree(jsonMessage);
            Set<ValidationMessage> validationErrors = schema.validate(message);
            validationErrors.forEach(error -> {
                instrumentation.logDebug("Message filtered out due to: {}", error.getMessage());
            });
            return validationErrors.isEmpty();
        } catch (JsonProcessingException e) {
            throw new FilterException("Failed to parse JSON message", e);
        }
    }

    private String deserialize(byte[] data) throws FilterException {
        switch (filterConfig.getFilterJsonMessageFormat()) {
            case PROTOBUF:
                try {
                    Object protoPojo = MethodUtils.invokeStaticMethod(Class.forName(filterConfig.getFilterSchemaProtoClass()), "parseFrom", data);
                    return jsonPrinter.print((GeneratedMessageV3) protoPojo);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new FilterException("Failed to parse Protobuf message", e);
                } catch (InvalidProtocolBufferException e) {
                    throw new FilterException("Protobuf message is invalid", e);
                } catch (NoSuchMethodException | ClassNotFoundException e) {
                    throw new FilterException("Proto schema class is invalid", e);
                }
            case JSON:
                return new String(data, Charset.defaultCharset());
            default:
                throw new FilterException("Invalid message format type");
        }
    }
}
