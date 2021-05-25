package io.odpf.firehose.sink.file;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.sink.file.proto.KafkaMetadataProto;
import io.odpf.firehose.sink.file.proto.KafkaMetadataProtoFile;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class DateHourPathFactoryTest {

    private final String zone = "Asia/Jakarta";
    private final String pattern = "YYYY-MM-dd";
    private final String fieldName = MessageProto.CREATED_TIME_FIELD_NAME;

    private DateHourPathFactory factory;
    private final String prefix = "dt=";

    private Instant timestamp = Instant.parse("2020-01-01T10:00:00.000Z");
    private int orderNumber = 100;
    private long offset = 1L;
    private int partition = 1;
    private String topic = "booking-log";
    private Path partitionPath = Paths.get("booking-log/dt=2020-01-01");

    @Test
    public void shouldCreatePath() {
        String kafkaMetadataFieldName = "";

        DynamicMessage message = createMessage(timestamp, orderNumber);
        DynamicMessage metadata = createMedatata(kafkaMetadataFieldName, timestamp, offset, partition, topic);
        Record record = new Record(message, metadata);

        factory = new DateHourPathFactory(kafkaMetadataFieldName, fieldName, pattern, zone, prefix);
        Path path = factory.create(record);

        assertEquals(partitionPath, path);
    }

    @Test
    public void shouldCreatePathForNestedMetadata() {
        String kafkaMetadataFieldName = "meta";

        DynamicMessage message = createMessage(timestamp, orderNumber);
        DynamicMessage metadata = createMedatata(kafkaMetadataFieldName, timestamp, offset, partition, topic);
        Record record = new Record(message, metadata);

        factory = new DateHourPathFactory(kafkaMetadataFieldName, fieldName, pattern, zone, prefix);
        Path path = factory.create(record);

        assertEquals(partitionPath, path);
    }

    private DynamicMessage createMedatata(String kafkaMetadataFieldName, Instant timestamp, long offset, int partition, String topic) {
        Message message = new Message("".getBytes(), "".getBytes(), topic, partition, offset,null,timestamp.toEpochMilli(),timestamp.toEpochMilli());
        KafkaMetadataFactory metadataFactory = new KafkaMetadataFactory(kafkaMetadataFieldName);
        return metadataFactory.create(message);
    }

    private DynamicMessage createMessage(Instant timestamp, int orderNumber) {
        MessageProto.MessageBuilder messageBuilder = MessageProto.createMessageBuilder();
        return messageBuilder
                .setCreatedTime(timestamp)
                .setOrderNumber(orderNumber).build();
    }
}