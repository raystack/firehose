package io.odpf.firehose.filter;

import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.config.enums.FilterType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * A concrete class of Filter. This class is responsible
 * for filtering the messages based on a filter condition.
 * <p>
 * The filter expression is obtained from the {@link KafkaConsumerConfig#getFilterJexlExpression()}
 * along with configurations for {@link KafkaConsumerConfig#getFilterJexlDataSource()} - [key|message]
 * and {@link KafkaConsumerConfig#getFilterJexlSchemaProtoClass()} - FQCN of the protobuf schema.
 */
public class MessageFilter implements Filter {

    private JexlEngine engine;
    private Expression expression;
    private FilterType filterType;
    private String protoSchema;
    private Instrumentation instrumentation;

    /**
     * Instantiates a new Message filter.
     *
     * @param consumerConfig  the consumer config
     * @param instrumentation the instrumentation
     */
    public MessageFilter(KafkaConsumerConfig consumerConfig, Instrumentation instrumentation) {
        this.engine = new JexlEngine();
        this.engine.setSilent(false);
        this.engine.setStrict(true);

        this.instrumentation = instrumentation;
        this.filterType = consumerConfig.getFilterJexlDataSource();
        this.protoSchema = consumerConfig.getFilterJexlSchemaProtoClass();

        this.instrumentation.logInfo("\n\tFilter type: {}", this.filterType);
        if (isNotNone(consumerConfig.getFilterJexlDataSource())) {
            this.expression = this.engine.createExpression(consumerConfig.getFilterJexlExpression());
            this.instrumentation.logInfo("\n\tFilter schema: {}", this.protoSchema);
            this.instrumentation.logInfo("\n\tFilter expression: {}", consumerConfig.getFilterJexlExpression());

        } else {
            this.instrumentation.logInfo("No filter is selected");
        }
    }

    /**
     * method to filter the EsbMessages.
     *
     * @param messages the protobuf records in binary format that are wrapped in {@link Message}
     * @return {@link Message}
     * @throws FilterException the filter exception
     */
    @Override
    public List<Message> filter(List<Message> messages) throws FilterException {
        if (isNone(filterType)) {
            return messages;
        } else {
            List<Message> filteredMessages = new ArrayList<>();

            for (Message message : messages) {
                try {
                    Object data = (filterType.equals(FilterType.KEY)) ? message.getLogKey() : message.getLogMessage();
                    Object obj = MethodUtils.invokeStaticMethod(Class.forName(protoSchema), "parseFrom", data);
                    if (evaluate(obj)) {
                        filteredMessages.add(message);
                    }
                } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                    throw new FilterException("Failed while filtering EsbMessages", e);
                }
            }

            return filteredMessages;
        }
    }

    private boolean evaluate(Object data) throws FilterException {
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

    private JexlContext convertDataToContext(Object t) throws IllegalAccessException {
        JexlContext context = new MapContext();

        context.set(getObjectAccessor(), t);

        return context;
    }

    private String getObjectAccessor() {
        String[] schemaNameSplit = protoSchema.split("\\.");
        String objectAccessor = schemaNameSplit[schemaNameSplit.length - 1];

        return objectAccessor.substring(0, 1).toLowerCase() + objectAccessor.substring(1);
    }

    private boolean isNone(FilterType filterTypeVal) {
        return filterTypeVal.equals(FilterType.NONE);
    }

    private boolean isNotNone(FilterType filterTypeVal) {
        return !isNone(filterTypeVal);
    }
}
