package io.odpf.firehose.sink.bigquery.handler;

import com.google.cloud.bigquery.InsertAllRequest;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.sink.bigquery.models.Record;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;


public class BigQueryRowWithInsertIdTest {

    @Test
    public void shouldCreateRowWithInsertID() {
        Message message = new Message("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8), "default", 1, 1);
        Record record = new Record(message, new HashMap<>());

        BigQueryRowWithInsertId withInsertId = new BigQueryRowWithInsertId();
        InsertAllRequest.RowToInsert rowToInsert = withInsertId.of(record);
        String id = rowToInsert.getId();

        assertEquals("default_1_1", id);
    }
}
