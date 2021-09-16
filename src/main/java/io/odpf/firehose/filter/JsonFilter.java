package io.odpf.firehose.filter;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.util.JsonFormat;
import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.config.enums.FilterDataSourceType;
import io.odpf.firehose.config.enums.FilterMessageType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

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
        filterDataSourceType = consumerConfig.getFilterJexlDataSource();
        protoSchema = consumerConfig.getFilterJexlSchemaProtoClass();
        messageType = consumerConfig.getFilterMessageType();

        this.instrumentation.logInfo("\n\tFilter type: {}", filterDataSourceType);
        if (isNotNone(consumerConfig.getFilterJexlDataSource())) {

            this.filterRule = consumerConfig.getFilterJsonRule();
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
        Object result;

        try {
            result = expression.evaluate(convertDataToContext(data));
        } catch (JexlException | IllegalAccessException e) {
            throw new FilterException("Failed while filtering " + e.getMessage());
        }

        if (result instanceof Boolean) {
            return (Boolean) result;
        } else {
            throw new FilterException("Expression should be correct!!");
        }
    }


    private boolean isNone(FilterDataSourceType filterDataSourceTypeVal) {
        return filterDataSourceTypeVal.equals(FilterDataSourceType.NONE);
    }

    private boolean isNotNone(FilterDataSourceType filterDataSourceTypeVal) {
        return !isNone(filterDataSourceTypeVal);
    }
}
