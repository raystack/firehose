package io.odpf.firehose.sink.bigquery.handler;

import io.odpf.firehose.consumer.Message;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class BQFilteredResponse {
    private final List<Message> sink5xxErrorMessages;
    private final List<Message> sink4xxErrorMessages;
    private final List<Message> sinkUnknownErrorMessages;
}
