package com.gojek.esb.sink.db.field.message;

import com.gojek.esb.sink.db.field.DBField;
import com.google.gson.GsonBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

public class DBCollectionField implements DBField {
    private Object columnValue;
    private Descriptors.FieldDescriptor fieldDescriptor;

    public DBCollectionField(Object columnValue, Descriptors.FieldDescriptor fieldDescriptor) {
        this.columnValue = columnValue;
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public Object getColumn() throws RuntimeException {
        Collection collectionOfMessages = (Collection) columnValue;
        Optional first = collectionOfMessages.stream().findFirst();
        if (first.isPresent() && first.get() instanceof Message) {
            Object messageJsons = collectionOfMessages
                    .stream()
                    .map(cValue -> new DBDefaultMessageField(cValue).getColumn().toString())
                    .collect(Collectors.joining(","));
            return "[" + messageJsons + "]";
        } else {
            return new GsonBuilder().create().toJson(collectionOfMessages);
        }
    }

    @Override
    public boolean canProcess() {
        return columnValue instanceof Collection && !fieldDescriptor.isMapField();
    }
}
