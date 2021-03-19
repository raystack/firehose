package io.odpf.firehose.sink.jdbc;

import io.odpf.firehose.sink.jdbc.field.JdbcFieldFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import lombok.Getter;

import java.util.Map;
import java.util.Properties;

public class JdbcMapper {
    private String key;
    private Message message;
    private Properties protoToDbMapping;
    @Getter
    private Object columnValue;
    @Getter
    private Object column;
    private Descriptors.FieldDescriptor fieldDescriptor;

    public JdbcMapper(String key, Message message, Properties protoToDbMapping) {
        this.key = key;
        this.message = message;
        this.protoToDbMapping = protoToDbMapping;
    }

    public JdbcMapper initialize() {
        Integer protoIndex = Integer.valueOf(key);
        columnValue = getField(protoIndex);
        column = protoToDbMapping.get(key);
        fieldDescriptor = message.getDescriptorForType().findFieldByNumber(protoIndex);
        return this;
    }

    private Object getField(Integer protoIndex) {
        return message.getField(message.getDescriptorForType().findFieldByNumber(protoIndex));
    }

    public Map<String, Object> add(Map<String, Object> columnToValueMap) {
        Object columnValueResult = JdbcFieldFactory
                .getField(this.columnValue, this.fieldDescriptor)
                .getColumn();
        columnToValueMap.put((String) column, columnValueResult);
        return columnToValueMap;
    }
}
