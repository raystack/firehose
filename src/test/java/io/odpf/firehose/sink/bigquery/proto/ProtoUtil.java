package io.odpf.firehose.sink.bigquery.proto;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import io.odpf.firehose.StatusBQ;
import io.odpf.firehose.TestMessageBQ;
import io.odpf.firehose.TestNestedMessageBQ;
import io.odpf.firehose.sink.bigquery.models.ProtoField;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ProtoUtil {
    private static final int TRIP_DURATION_NANOS = 1000000000;
    private static int call = 0;

    public static TestMessageBQ generateTestMessage(Instant now) {
        call++;
        Timestamp createdAt = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        return TestMessageBQ.newBuilder()
                .setOrderNumber("order-" + call)
                .setOrderUrl("order-url-" + call)
                .setOrderDetails("order-details-" + call)
                .setCreatedAt(createdAt)
                .setStatus(StatusBQ.COMPLETED)
                .setTripDuration(Duration.newBuilder().setSeconds(1).setNanos(TRIP_DURATION_NANOS).build())
                .addUpdatedAt(createdAt)
                .addUpdatedAt(createdAt)
                .build();

    }

    public static TestNestedMessageBQ generateTestNestedMessage(String nestedId, TestMessageBQ message) {
        return TestNestedMessageBQ.newBuilder()
                .setSingleMessage(message)
                .setNestedId(nestedId)
                .build();
    }

    public static ProtoField createProtoField(String name, DescriptorProtos.FieldDescriptorProto.Type type, DescriptorProtos.FieldDescriptorProto.Label label) {
        return new ProtoField(name, "", type, label, new ArrayList<>(), 0);
    }

    public static ProtoField createProtoField(List<ProtoField> subFields) {
        return new ProtoField("", "", null, null, subFields, 0);
    }

    public static ProtoField createProtoField(String name, String typeName, DescriptorProtos.FieldDescriptorProto.Type type, DescriptorProtos.FieldDescriptorProto.Label label) {
        return new ProtoField(name, typeName, type, label, new ArrayList<>(), 0);
    }

    public static ProtoField createProtoField(String name, String typeName, DescriptorProtos.FieldDescriptorProto.Type type, DescriptorProtos.FieldDescriptorProto.Label label, List<ProtoField> fields) {
        return new ProtoField(name, typeName, type, label, fields, 0);
    }

    public static ProtoField createProtoField(String name, int index) {
        return new ProtoField(name, "", null, null, new ArrayList<>(), index);
    }

    public static ProtoField createProtoField(String name, String typeName, DescriptorProtos.FieldDescriptorProto.Type type, int index, List<ProtoField> fields) {
        return new ProtoField(name, typeName, type, null, fields, index);
    }
}
