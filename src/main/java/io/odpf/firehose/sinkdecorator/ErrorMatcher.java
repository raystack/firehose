package io.odpf.firehose.sinkdecorator;

import io.odpf.firehose.consumer.ErrorInfo;
import io.odpf.firehose.consumer.ErrorType;
import io.odpf.firehose.consumer.Message;
import lombok.AllArgsConstructor;

import java.util.Set;

@AllArgsConstructor
public class ErrorMatcher {

    private final boolean nullErrorIncluded;
    private final Set<ErrorType> errorTypes;

    public boolean isMatch(Message message) {
        if (message.getErrorInfo() != null) {
            ErrorInfo errorInfo = message.getErrorInfo();
            if (errorInfo.getErrorType() != null) {
                return errorTypes.contains(errorInfo.getErrorType());
            }
        }

        return nullErrorIncluded;
    }
}
