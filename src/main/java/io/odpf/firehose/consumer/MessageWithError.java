package io.odpf.firehose.consumer;


import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * class that hold message and the exception thrown when message being processed.
 */
@Data
@AllArgsConstructor
public class MessageWithError {
    private Message message;
    private ErrorType errorType;
}
