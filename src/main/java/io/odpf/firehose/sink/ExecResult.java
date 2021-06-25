package io.odpf.firehose.sink;

import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.MessageWithError;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class ExecResult {
    private List<Message> retryAbleMessages;
    private List<MessageWithError> deadLetterQueue;
}
