package io.odpf.firehose.sink.bigquery.handler;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllResponse;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.sink.bigquery.error.BQRecordsErrorType;
import io.odpf.firehose.sink.bigquery.error.ErrorTypeFactory;
import io.odpf.firehose.sink.bigquery.exception.BigQuerySinkException;
import io.odpf.firehose.sink.bigquery.models.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class BigQueryResponseParser {
    /**
     * Parses the {@link InsertAllResponse} object and returns {@link Message} that were
     * tried to sink in BQ and the error type {@link BQRecordsErrorType}.
     * {@link InsertAllResponse} in bqResponse are 1 to 1 indexed based on the records that are requested to be inserted.
     *
     * @param records    - list of records that were tried with BQ insertion
     * @param bqResponse - the status of insertion for all records as returned by BQ
     * @return list of messages with error.
     */
    public static List<Message> parseResponse(final List<Record> records, final InsertAllResponse bqResponse) {
        if (!bqResponse.hasErrors()) {
            return Collections.emptyList();
        }
        List<Message> messages = new ArrayList<>();
        Map<Long, List<BigQueryError>> insertErrorsMap = bqResponse.getInsertErrors();
        for (final Map.Entry<Long, List<BigQueryError>> errorEntry : insertErrorsMap.entrySet()) {
            final Message message = records.get(errorEntry.getKey().intValue()).getMessage();
            Set<BQRecordsErrorType> errorTypes = errorTypeList(errorEntry.getValue());
            if (errorTypes.contains(BQRecordsErrorType.UNKNOWN)) {
                message.setErrorInfo(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_UNKNOWN_ERROR));
            } else if (errorTypes.contains(BQRecordsErrorType.INVALID) || errorTypes.contains(BQRecordsErrorType.OOB)) {
                message.setErrorInfo(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_4XX_ERROR));
            } else if (errorTypes.contains(BQRecordsErrorType.VALID)) {
                message.setErrorInfo(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_5XX_ERROR));
            }
            messages.add(message);
        }
        return messages;
    }

    private static Set<BQRecordsErrorType> errorTypeList(List<BigQueryError> bqErrors) {
        return bqErrors.stream().map(err ->
                ErrorTypeFactory.getErrorType(err.getReason(), err.getMessage())).
                collect(Collectors.toSet());
    }
}
