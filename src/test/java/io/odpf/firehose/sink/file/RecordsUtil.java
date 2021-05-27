package io.odpf.firehose.sink.file;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import io.odpf.firehose.sink.file.message.Record;

public class RecordsUtil {
    public static Record createRecord(String msgValue, int msgMetadata) {
        DynamicMessage message = DynamicMessage.newBuilder(StringValue.of(msgValue)).build();
        DynamicMessage metadata = DynamicMessage.newBuilder(Int64Value.of(msgMetadata)).build();
        return new Record(message, metadata);
    }
}
