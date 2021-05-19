package io.odpf.firehose.sink.file;

import com.google.protobuf.DynamicMessage;
import lombok.*;


@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Record {
    private DynamicMessage message;
    private DynamicMessage metadata;
}
