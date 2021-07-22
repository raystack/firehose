package io.odpf.firehose.sink.bigquery.error;


import java.util.Arrays;
import java.util.List;

/**
 * Factory class that determines the type {@link BQRecordsErrorType} error based on the
 * error string supplied.
 */
public class ErrorTypeFactory {

    public static BQRecordsErrorType getErrorType(String reasonText, String msgText) {
        List<ErrorDescriptor> errDescList = Arrays.asList(
                new InvalidSchemaError(reasonText, msgText),
                new OOBError(reasonText, msgText),
                new StoppedError(reasonText));

        ErrorDescriptor errorDescriptor = errDescList
                .stream()
                .filter(ErrorDescriptor::matches)
                .findFirst()
                .orElse(new UnknownError());
        return errorDescriptor.getType();
    }

}
