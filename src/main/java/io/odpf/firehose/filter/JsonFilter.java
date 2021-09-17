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
    private Instrumentation instrumentation;
    private String filterRule;
    private final FilterMessageType messageType;

    public JsonFilter(KafkaConsumerConfig consumerConfig, Instrumentation instrumentation) {

        this.instrumentation = instrumentation;
        filterDataSourceType = consumerConfig.getFilterJsonDataSource();
        protoSchema = consumerConfig.getFilterJsonSchemaProtoClass();
        messageType = consumerConfig.getFilterMessageType();

        this.instrumentation.logInfo("\n\tFilter type: {}", filterDataSourceType);
        if (isNotNone(consumerConfig.getFilterJsonDataSource())) {

            this.filterRule = consumerConfig.getFilterJsonSchema();
            this.instrumentation.logInfo("\n\tFilter schema: {}", protoSchema);
            this.instrumentation.logInfo("\n\tFilter expression: {}", filterRule);

        } else {
            this.instrumentation.logInfo("No filter is selected");
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

        } else {
            List<Message> filteredMessages = new ArrayList<>();
            for (Message message : messages) {
                byte[] data = (filterDataSourceType.equals(KEY)) ? message.getLogKey() : message.getLogMessage();
                String ss = "";

                if (messageType == PROTOBUF) {
                    try {
                        Object obj = MethodUtils.invokeStaticMethod(Class.forName(protoSchema), "parseFrom", data);
                        JsonFormat.Printer d = JsonFormat.printer().preservingProtoFieldNames();
                        ss = d.print((GeneratedMessageV3) obj);

                    } catch (Exception e) {
                        throw new FilterException("Failed while filtering EsbMessages", e);
                    }
                } else if (messageType == JSON) {
                    ss = new String(data, Charset.defaultCharset());

                }
                if (evaluate(ss, filterRule)) {
                    filteredMessages.add(message);
                }
            }
            return filteredMessages;
        }
    }


    private boolean evaluate(String data, String rule) throws FilterException {

        ObjectMapper objectMapper = new ObjectMapper();
        JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);

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
