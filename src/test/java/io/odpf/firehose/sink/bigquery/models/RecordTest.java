package io.odpf.firehose.sink.bigquery.models;

import io.odpf.firehose.consumer.Message;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class RecordTest {

    @Test
    public void shouldReturnID() {
        Message message = new Message("123".getBytes(StandardCharsets.UTF_8), "abc".getBytes(StandardCharsets.UTF_8), "default", 1, 1);
        Record record = new Record(message, null);
        String id = record.getId();

        assertEquals("default_1_1", id);
    }

    @Test
    public void shouldReturnSize() {
        Message message = new Message("123".getBytes(StandardCharsets.UTF_8), "abc".getBytes(StandardCharsets.UTF_8), "default", 1, 1);
        Record record = new Record(message, null);

        long size = record.getSize();

        assertEquals(6, size);
    }
}
