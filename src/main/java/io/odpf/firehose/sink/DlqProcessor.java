package io.odpf.firehose.sink;

import io.odpf.firehose.consumer.MessageWithError;

import java.io.IOException;
import java.util.List;

public interface DlqProcessor {
    ExecResult executeWithError() throws Exception;

    List<MessageWithError> processDlq(List<MessageWithError> messages) throws IOException;
}
