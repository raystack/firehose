package com.gojek.esb.sink.db.field.message;

import com.gojek.esb.sink.db.field.DBField;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class DBTimestampField implements DBField {
    private Object columnValue;

    public DBTimestampField(Object columnValue) {
        this.columnValue = columnValue;
    }

    @Override
    public Object getColumn() {
        List<Descriptors.FieldDescriptor> fieldDescriptors = ((DynamicMessage) columnValue).getDescriptorForType().getFields();
        ArrayList<Object> timeFields = new ArrayList<>();
        for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
            timeFields.add(((DynamicMessage) columnValue).getField(fieldDescriptor));
        }
        Instant instant = Instant.ofEpochSecond((long) timeFields.get(0), ((Integer) timeFields.get(1)).longValue());
        return instant;
    }

    @Override
    public boolean canProcess() {
        return columnValue instanceof DynamicMessage && ((DynamicMessage) columnValue).getDescriptorForType().getName().equals(Timestamp.class.getSimpleName());

    }
}
