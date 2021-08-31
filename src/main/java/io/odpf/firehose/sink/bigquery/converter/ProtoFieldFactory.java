package io.odpf.firehose.sink.bigquery.converter;

import com.google.protobuf.Descriptors;
import io.odpf.firehose.sink.bigquery.converter.fields.ByteProtoField;
import io.odpf.firehose.sink.bigquery.converter.fields.DefaultProtoField;
import io.odpf.firehose.sink.bigquery.converter.fields.EnumProtoField;
import io.odpf.firehose.sink.bigquery.converter.fields.NestedProtoField;
import io.odpf.firehose.sink.bigquery.converter.fields.ProtoField;
import io.odpf.firehose.sink.bigquery.converter.fields.StructProtoField;
import io.odpf.firehose.sink.bigquery.converter.fields.TimeStampProtoField;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ProtoFieldFactory {

    public static ProtoField getField(Descriptors.FieldDescriptor descriptor, Object fieldValue) {
        List<ProtoField> protoFields = Arrays.asList(
                new TimeStampProtoField(descriptor, fieldValue),
                new EnumProtoField(descriptor, fieldValue),
                new ByteProtoField(descriptor, fieldValue),
                new StructProtoField(descriptor, fieldValue),
                new NestedProtoField(descriptor, fieldValue)
        );
        Optional<ProtoField> first = protoFields
                .stream()
                .filter(ProtoField::matches)
                .findFirst();
        return first.orElseGet(() -> new DefaultProtoField(descriptor, fieldValue));
    }

}
