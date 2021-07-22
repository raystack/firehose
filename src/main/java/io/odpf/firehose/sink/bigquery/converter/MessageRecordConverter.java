package io.odpf.firehose.sink.bigquery.converter;

import com.gojek.de.stencil.parser.Parser;
import com.google.api.client.util.DateTime;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.firehose.config.BigQuerySinkConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.sink.bigquery.models.Constants;
import io.odpf.firehose.sink.bigquery.models.Record;
import io.odpf.firehose.sink.bigquery.models.Records;
import io.odpf.firehose.sink.bigquery.proto.UnknownProtoFields;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Slf4j
public class MessageRecordConverter {
    private final RowMapper rowMapper;
    private final Parser parser;
    private final BigQuerySinkConfig config;

    public Records convert(List<Message> messages, Instant now) throws InvalidProtocolBufferException {
        ArrayList<Record> validRecords = new ArrayList<>();
        ArrayList<Record> invalidRecords = new ArrayList<>();
        for (Message message : messages) {
            if (message.getLogMessage() == null) {
                if (config.getFailOnNullMessage()) {
                    throw new RuntimeException("Null message " + message.getOffset());
                }
                continue;
            }
            Map<String, Object> columns = mapToColumns(message);
            if (columns.isEmpty()) {
                invalidRecords.add(new Record(message, columns));
                continue;
            }
            addMetadata(columns, message, now);
            validRecords.add(new Record(message, columns));
        }
        return new Records(validRecords, invalidRecords);
    }

    private Map<String, Object> mapToColumns(Message message) throws InvalidProtocolBufferException {
        Map<String, Object> columns = Collections.emptyMap();
        try {
            columns = rowMapper.map(parser.parse(message.getLogMessage()));
        } catch (InvalidProtocolBufferException e) {
            message.setErrorInfo(new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR));
            log.info("failed to deserialize message: {} at offset: {}, partition: {}", UnknownProtoFields.toString(message.getLogMessage()),
                    message.getOffset(), message.getPartition());
            if (config.getFailOnDeserializeError()) {
                throw new InvalidProtocolBufferException(e);
            }
        }
        return columns;
    }

    private void addMetadata(Map<String, Object> columns, Message message, Instant now) {
        Map<String, Object> offsetMetadata = new HashMap<>();
        offsetMetadata.put(Constants.PARTITION_COLUMN_NAME, message.getPartition());
        offsetMetadata.put(Constants.OFFSET_COLUMN_NAME, message.getOffset());
        offsetMetadata.put(Constants.TOPIC_COLUMN_NAME, message.getTopic());
        offsetMetadata.put(Constants.TIMESTAMP_COLUMN_NAME, new DateTime(message.getTimestamp()));
        offsetMetadata.put(Constants.LOAD_TIME_COLUMN_NAME, new DateTime(Date.from(now)));

        if (config.getBqMetadataNamespace().isEmpty()) {
            columns.putAll(offsetMetadata);
        } else {
            columns.put(config.getBqMetadataNamespace(), offsetMetadata);
        }
    }
}
