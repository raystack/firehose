package io.odpf.firehose.filter.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import io.odpf.firehose.config.FilterConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.filter.Filter;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.filter.FilteredMessages;
import io.odpf.firehose.metrics.Instrumentation;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

import static io.odpf.firehose.config.enums.FilterDataSourceType.KEY;

/**
 * JSON-based filter to filter protobuf/JSON messages based on rules
 * defined in a JSON Schema string.
 */
public class JsonFilter implements Filter {

    private final ProtoParser parser;
    private final FilterConfig filterConfig;
    private final Instrumentation instrumentation;
    private final JsonSchema schema;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonFormat.Printer jsonPrinter;

    /**
     * Instantiates a new Json filter.
     *
     * @param filterConfig    the consumer config
     * @param instrumentation the instrumentation
     */
    public JsonFilter(StencilClient stencilClient, FilterConfig filterConfig, Instrumentation instrumentation) {
        this.parser = new ProtoParser(stencilClient, filterConfig.getFilterSchemaProtoClass());
        this.instrumentation = instrumentation;
        this.filterConfig = filterConfig;
        JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        this.schema = schemaFactory.getSchema(filterConfig.getFilterJsonSchema());
        this.jsonPrinter = JsonFormat.printer().preservingProtoFieldNames();
    }

    /**
     * method to filter the EsbMessages.
     *
     * @param messages the json/protobuf records in binary format that are wrapped in {@link Message}
     * @return the list of filtered Messages
     * @throws FilterException the filter exception
     */
    @Override
    public FilteredMessages filter(List<Message> messages) throws FilterException {
        FilteredMessages filteredMessages = new FilteredMessages();
        for (Message message : messages) {
            byte[] data = (filterConfig.getFilterDataSource().equals(KEY)) ? message.getLogKey() : message.getLogMessage();
            String jsonMessage = deserialize(data);
            if (evaluate(jsonMessage)) {
                filteredMessages.addToValidMessages(message);
            } else {
                filteredMessages.addToInvalidMessages(message);
            }
        }
        return filteredMessages;
    }

    private boolean evaluate(String jsonMessage) throws FilterException {
        try {
            JsonNode message = objectMapper.readTree(jsonMessage);
            if (instrumentation.isDebugEnabled()) {
                instrumentation.logDebug("Json Message: \n {}", message.toPrettyString());
            }
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
                    DynamicMessage message = parser.parse(data);
                    return jsonPrinter.print(message);

                } catch (Exception e) {
                    throw new FilterException("Failed to parse Protobuf message", e);
                }
            case JSON:
                return new String(data, Charset.defaultCharset());
            default:
                throw new FilterException("Invalid message format type");
        }
    }
}
