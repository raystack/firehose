package com.gojek.esb.sink.jdbc.field;

import com.gojek.esb.sink.jdbc.field.message.JdbcCollectionField;
import com.gojek.esb.sink.jdbc.field.message.JdbcDefaultMessageField;
import com.gojek.esb.sink.jdbc.field.message.JdbcTimestampField;
import com.google.protobuf.Descriptors;

import java.util.Arrays;
import java.util.List;

public class JdbcFieldFactory {
    public static JdbcField getField(Object columnValue, Descriptors.FieldDescriptor fieldDescriptor) {
        List<JdbcField> jdbcFields = Arrays.asList(
                new JdbcCollectionField(columnValue, fieldDescriptor),
                new JdbcMapField(columnValue, fieldDescriptor),
                new JdbcTimestampField(columnValue),
                new JdbcDefaultMessageField(columnValue));
        return jdbcFields.stream()
                .filter(JdbcField::canProcess)
                .findFirst()
                .orElseGet(() -> new JdbcDefaultField(columnValue));
    }

}
