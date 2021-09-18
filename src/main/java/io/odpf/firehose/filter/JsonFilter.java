package io.odpf.firehose.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.util.JsonFormat;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaException;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.config.enums.FilterDataSourceType;
import io.odpf.firehose.config.enums.FilterMessageType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.odpf.firehose.config.enums.FilterDataSourceType.KEY;
import static io.odpf.firehose.config.enums.FilterMessageType.JSON;
import static io.odpf.firehose.config.enums.FilterMessageType.PROTOBUF;

public class JsonFilter implements Filter {

    private final FilterDataSourceType filterDataSourceType;
    private final String protoSchema;
    private String filterRule;
    private final FilterMessageType messageType;
    private final ObjectMapper objectMapper;
    private final JsonSchemaFactory schemaFactory;
    private Instrumentation instrumentation;
    private Method parser;


    public JsonFilter(KafkaConsumerConfig consumerConfig, Instrumentation instrumentation) {
        objectMapper = new ObjectMapper();
        schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);

        filterDataSourceType = consumerConfig.getFilterJsonDataSource();
        protoSchema = consumerConfig.getFilterJsonSchemaProtoClass();
        messageType = consumerConfig.getFilterMessageType();

        if (messageType == PROTOBUF) {
            try {
                parser = MethodUtils.getAccessibleMethod(Class.forName(protoSchema), "parseFrom", byte[].class);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        this.instrumentation = instrumentation;
        instrumentation.logInfo("\n\tFilter type: {}", filterDataSourceType);
        if (isNotNone(consumerConfig.getFilterJsonDataSource())) {

            this.filterRule = consumerConfig.getFilterJsonSchema();
            instrumentation.logInfo("\n\tFilter schema: {}", protoSchema);
            instrumentation.logInfo("\n\tFilter expression: {}", filterRule);

        } else {
            instrumentation.logInfo("No filter is selected");
        }
    }

    /**
     * method to filter the EsbMessages.
     *
     * @param messages the json/protobuf records in binary format that are wrapped in {@link Message}
     * @return {@link Message}
     * @throws FilterException the filter exception
     */
    @Override
    public List<Message> filter(List<Message> messages) throws FilterException {

        if (isNone(filterDataSourceType)) {
            return messages;
        }

        List<Message> filteredMessages = new ArrayList<>();
        for (Message message : messages) {
            byte[] data = (filterDataSourceType.equals(KEY)) ? message.getLogKey() : message.getLogMessage();
            String jsonMessage = "";

            if (messageType == PROTOBUF) {
                try {
                    Object obj = parser.invoke(null, data);
                    JsonFormat.Printer printer = JsonFormat.printer().preservingProtoFieldNames();
                    jsonMessage = printer.print((GeneratedMessageV3) obj);

                } catch (Exception e) {
                    throw new FilterException("Failed while filtering EsbMessages", e);
                }
            } else if (messageType == JSON) {
                jsonMessage = new String(data, Charset.defaultCharset());
            }
            if (evaluate(jsonMessage, filterRule)) {
                filteredMessages.add(message);
            }
        }
        return filteredMessages;
    }


    private boolean evaluate(String data, String rule) throws FilterException {

        JsonNode json;
        JsonSchema schema;
        try {
            json = objectMapper.readTree(data);
            schema = schemaFactory.getSchema(rule);
        } catch (JsonProcessingException | JsonSchemaException e) {
            throw new FilterException("Failed while filtering " + e.getMessage());
        }
        Set<ValidationMessage> validationResult = schema.validate(json);

        return validationResult.isEmpty();
    }


    private boolean isNone(FilterDataSourceType filterDataSourceTypeVal) {
        return filterDataSourceTypeVal.equals(FilterDataSourceType.NONE);
    }

    private boolean isNotNone(FilterDataSourceType filterDataSourceTypeVal) {
        return !isNone(filterDataSourceTypeVal);
    }
}
