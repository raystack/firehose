package io.odpf.firehose.sink.bigquery.error;

import lombok.AllArgsConstructor;

@AllArgsConstructor
/**
 * UnknownError is used when error factory failed to match any possible
 * known errors
 * */
public class UnknownError implements ErrorDescriptor {

    @Override
    public BQRecordsErrorType getType() {
        return BQRecordsErrorType.UNKNOWN;
    }

    @Override
    public boolean matches() {
        return false;
    }
}
