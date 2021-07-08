package io.odpf.firehose.consumer;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class ErrorInfo {
    private Exception exception;
    private ErrorType errorType;
}
