package io.odpf.firehose.sink.bigquery.converter.fields;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class NestedField implements ProtoField {
    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    @Override
    public DynamicMessage getValue() {
        return (DynamicMessage) fieldValue;
    }

    @Override
    public boolean matches() {
        return descriptor.getJavaType().name().equals("MESSAGE")
                && !(fieldValue instanceof List);
    }
}
