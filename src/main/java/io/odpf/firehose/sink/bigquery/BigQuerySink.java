package io.odpf.firehose.sink.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.bigquery.converter.MessageRecordConverterCache;
import io.odpf.firehose.sink.bigquery.handler.BigQueryRow;
import io.odpf.firehose.sink.bigquery.models.Record;
import io.odpf.firehose.sink.bigquery.models.Records;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class BigQuerySink extends AbstractSink {

    private final BigQuery bigQueryInstance;
    private final TableId tableId;
    private final BigQueryRow rowCreator;
    private final Instrumentation instrumentation;
    private final MessageRecordConverterCache converterCache;
    private List<Message> messageList;

    public BigQuerySink(Instrumentation instrumentation,
                        String sinkType,
                        BigQuery bigQueryInstance,
                        TableId tableId,
                        MessageRecordConverterCache converterCache,
                        BigQueryRow rowCreator) {
        super(instrumentation, sinkType);
        this.instrumentation = instrumentation;
        this.bigQueryInstance = bigQueryInstance;
        this.tableId = tableId;
        this.converterCache = converterCache;
        this.rowCreator = rowCreator;
    }

    @Override
    protected List<Message> execute() throws Exception {
        Instant now = Instant.now();
        System.out.println("**** Converting ***");
        Records records = converterCache.getMessageRecordConverter().convert(messageList, now);
        System.out.println("**** SENDING ***");
        InsertAllResponse response = insertIntoBQ(records.getValidRecords());
        System.out.println("**** FINISHED ***");
        System.out.println(response);
        //parse the response.
        return new ArrayList<>();
    }

    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
        this.messageList = messages;
    }

    @Override
    public void close() throws IOException {

    }

    private InsertAllResponse insertIntoBQ(List<Record> records) {
        Instant start = Instant.now();
        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);
        records.forEach((Record m) -> builder.addRow(rowCreator.of(m)));
        InsertAllRequest rows = builder.build();
        InsertAllResponse response = bigQueryInstance.insertAll(rows);

        instrumentation.logInfo("Pushed a batch of {} records to BQ. Insert success?: {}", records.size(), !response.hasErrors());
        records.forEach(m -> instrumentation.incrementCounter("bq.sink.push.records"));
        instrumentation.captureDurationSince("bq.sink.push.time", start);
        return response;
    }
}
