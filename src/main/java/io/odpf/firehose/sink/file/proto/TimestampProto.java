package io.odpf.firehose.sink.file.proto;

import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Timestamp;

public class TimestampProto {
    private static final String TYPE_NAME = "Timestamp";
    public static final String SECONDS_FIELD_NAME = "seconds";
    public static final String NANOS_FIELD_NAME = "nanos";

    public static MessageDefinition createMessageDefinition(){
        return MessageDefinition.newBuilder(TYPE_NAME)
                .addField("optional", "int64", SECONDS_FIELD_NAME, 1)
                .addField("optional", "int32", NANOS_FIELD_NAME, 2)
                .build();
    }

    public static String getTypeName() {
        return TYPE_NAME;
    }

    public static Timestamp.Builder newBuilder(){
        return Timestamp.newBuilder();
    }
}
