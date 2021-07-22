package io.odpf.firehose.sink.bigquery.error;

/**
 * Models the Big Query records status.
 */
public enum BQRecordsErrorType {

    /**
     * BQ failures with valid data.
     */
    VALID,

    /**
     * BQ failures due to Out of Bounds errors on partition keys.
     */
    OOB,

    /**
     * BQ errors due to invalid schema/data.
     */
    INVALID,

    /**
     * Unknown error type.
     */
    UNKNOWN,
}
