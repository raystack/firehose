package com.gojek.esb.filter;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.config.enums.EsbFilterType;
import com.gojek.esb.consumer.EsbMessage;
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
 * The filter expression is obtained from the {@link KafkaConsumerConfig#getFilterExpression()}
 * along with configurations for {@link KafkaConsumerConfig#getFilterType()} - [key|message]
 * and {@link KafkaConsumerConfig#getFilterProtoSchema()} - FQCN of the protobuf schema.
 */
public class EsbMessageFilter implements Filter {

    private JexlEngine engine;
    private Expression expression;
    private EsbFilterType filterType;
    private String protoSchema;

    public EsbMessageFilter(KafkaConsumerConfig consumerConfig) {
        this.engine = new JexlEngine();
        this.engine.setSilent(false);
        this.engine.setStrict(true);

        this.filterType = consumerConfig.getFilterType();
        this.protoSchema = consumerConfig.getFilterProtoSchema();

        if (isNotNone(consumerConfig.getFilterType())) {
            this.expression = this.engine.createExpression(consumerConfig.getFilterExpression());
        }
    }

    /**
     * method to filter the EsbMessages.
     * @param messages the protobuf records in binary format that are wrapped in {@link EsbMessage}
     * @return {@link EsbMessage}
     * @throws EsbFilterException
     */
    @Override
    public List<EsbMessage> filter(List<EsbMessage> messages) throws EsbFilterException {
        if (isNone(filterType)) {
            return messages;
        } else {
            List<EsbMessage> filteredMessages = new ArrayList<>();

            for (EsbMessage message : messages) {
                try {
                    Object data = (filterType.equals(EsbFilterType.KEY)) ? message.getLogKey() : message.getLogMessage();
                    Object obj = MethodUtils.invokeStaticMethod(Class.forName(protoSchema), "parseFrom", data);
                    if (evaluate(obj)) {
                        filteredMessages.add(message);
                    }
                } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                    throw new EsbFilterException("Failed while filtering EsbMessages", e);
                }
            }

            return filteredMessages;
        }
    }

    private boolean evaluate(Object data) throws EsbFilterException {
        Object result;

        try {
            result = expression.evaluate(convertDataToContext(data));
        } catch (JexlException | IllegalAccessException e) {
            throw new EsbFilterException("Failed while filtering " + e.getMessage());
        }

        if (result instanceof Boolean) {
            return (Boolean) result;
        } else {
            throw new EsbFilterException("Expression should be correct!!");
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

    private boolean isNone(EsbFilterType filterTypeVal) {
        return filterTypeVal.equals(EsbFilterType.NONE);
    }

    private boolean isNotNone(EsbFilterType filterTypeVal) {
        return !isNone(filterTypeVal);
    }
}
