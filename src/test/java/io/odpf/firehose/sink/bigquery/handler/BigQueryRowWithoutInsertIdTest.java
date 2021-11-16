package io.odpf.firehose.sink.bigquery.handler;

import com.google.cloud.bigquery.InsertAllRequest;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.sink.bigquery.models.Record;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import static org.junit.Assert.*;

public class BigQueryRowWithoutInsertIdTest {

    @Test
    public void shouldCreateRowWithoutInsertID() {
        Message message = new Message("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8), "default", 1, 1);
        Record record = new Record(message, new HashMap<>());

        BigQueryRowWithoutInsertId withoutInsertId = new BigQueryRowWithoutInsertId();
        InsertAllRequest.RowToInsert rowToInsert = withoutInsertId.of(record);
        String id = rowToInsert.getId();

        assertNull(id);
    }
}
