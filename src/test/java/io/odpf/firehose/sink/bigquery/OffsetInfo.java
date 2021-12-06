package io.odpf.firehose.sink.bigquery;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@Getter
@AllArgsConstructor
public class OffsetInfo {
    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
}
