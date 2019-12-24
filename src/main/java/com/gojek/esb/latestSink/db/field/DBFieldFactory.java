package com.gojek.esb.latestSink.db.field;

import com.gojek.esb.latestSink.db.field.message.DBCollectionField;
import com.gojek.esb.latestSink.db.field.message.DBDefaultMessageField;
import com.gojek.esb.latestSink.db.field.message.DBTimestampField;
import com.google.protobuf.Descriptors;

import java.util.Arrays;
import java.util.List;

public class DBFieldFactory {
    public static DBField getField(Object columnValue, Descriptors.FieldDescriptor fieldDescriptor) {
        List<DBField> dbFields = Arrays.asList(
                new DBCollectionField(columnValue, fieldDescriptor),
                new DBMapField(columnValue, fieldDescriptor),
                new DBTimestampField(columnValue),
                new DBDefaultMessageField(columnValue));
        return dbFields.stream()
                .filter(DBField::canProcess)
                .findFirst()
                .orElseGet(() -> new DBDefaultField(columnValue));
    }

}
