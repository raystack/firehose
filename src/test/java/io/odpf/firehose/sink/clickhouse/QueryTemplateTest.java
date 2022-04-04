package io.odpf.firehose.sink.clickhouse;

import io.odpf.firehose.config.ClickhouseSinkConfig;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.proto.ProtoToFieldMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class QueryTemplateTest {

    @Mock
    private ClickhouseSinkConfig clickhouseSinkConfig;

    @Mock
    private ProtoToFieldMapper protoToFieldMapper;

    @Before
    public void setup() {
        Properties properties = new Properties();
        properties.put("1", "order_number");
        properties.put("2", "event_timestamp");
        properties.put("3", "feedback_rating");
        when(clickhouseSinkConfig.getInputSchemaProtoToColumnMapping()).thenReturn(properties);

        Map<String, Object> columnToValues = new HashMap<>();
        columnToValues.put("order_number", "order_1");
        columnToValues.put("event_timestamp", "ts1");
        columnToValues.put("feedback_rating", 5);
        when(protoToFieldMapper.getFields(any(byte[].class))).thenReturn(columnToValues);
    }

    @Test
    public void testToQueryString() {
        when(clickhouseSinkConfig.getKafkaRecordParserMode()).thenReturn("message");

        when(clickhouseSinkConfig.getClickhouseTableName()).thenReturn("table");
        QueryTemplate queryTemplate = new QueryTemplate(clickhouseSinkConfig, protoToFieldMapper);
        Message message = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);

        String actualGeneratedString = queryTemplate.toQueryStringForSingleMessage(message);
        assertEquals("INSERT INTO table ( feedback_rating,event_timestamp,order_number ) values ( '5', 'ts1', 'order_1' ) ",
                actualGeneratedString);

    }

    @Test
    public void testToQueryStringForKeyMessage() {
        when(clickhouseSinkConfig.getKafkaRecordParserMode()).thenReturn("key");

        when(clickhouseSinkConfig.getClickhouseTableName()).thenReturn("table");
        QueryTemplate queryTemplate = new QueryTemplate(clickhouseSinkConfig, protoToFieldMapper);
        Message message = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);

        String actualGeneratedString = queryTemplate.toQueryStringForSingleMessage(message);
        assertEquals("INSERT INTO table ( feedback_rating,event_timestamp,order_number ) values ( '5', 'ts1', 'order_1' ) ",
                actualGeneratedString);

    }


    @Test
    public void testToQueryStringForMultipleMessages() {
        when(clickhouseSinkConfig.getKafkaRecordParserMode()).thenReturn("message");
        when(clickhouseSinkConfig.getClickhouseTableName()).thenReturn("table");
        QueryTemplate queryTemplate = new QueryTemplate(clickhouseSinkConfig, protoToFieldMapper);
        Message message1 = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);
        Message message2 = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);

        List<Message> messageList = new ArrayList<>();
        messageList.add(message1);
        messageList.add(message2);

        String actualGeneratedString = queryTemplate.toQueryStringForMultipleMessages(messageList);
        assertEquals("INSERT INTO table ( feedback_rating,event_timestamp,order_number ) values ( '5', 'ts1', 'order_1' ),( '5', 'ts1', 'order_1' ) ",
                actualGeneratedString);

    }

    @Test
    public void testToQueryStringForMultipleMessagesForKeyMessage() {
        when(clickhouseSinkConfig.getKafkaRecordParserMode()).thenReturn("key");
        when(clickhouseSinkConfig.getClickhouseTableName()).thenReturn("table");
        QueryTemplate queryTemplate = new QueryTemplate(clickhouseSinkConfig, protoToFieldMapper);
        Message message1 = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);
        Message message2 = new Message("key".getBytes(), "msg".getBytes(), "topic1", 0, 100);

        List<Message> messageList = new ArrayList<>();
        messageList.add(message1);
        messageList.add(message2);

        String actualGeneratedString = queryTemplate.toQueryStringForMultipleMessages(messageList);
        assertEquals("INSERT INTO table ( feedback_rating,event_timestamp,order_number ) values ( '5', 'ts1', 'order_1' ),( '5', 'ts1', 'order_1' ) ",
                actualGeneratedString);

    }
}
