package io.odpf.firehose.sink.objectstorage.writer.local;

import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.sink.objectstorage.Constants;
import io.odpf.firehose.sink.objectstorage.TestUtils;
import io.odpf.firehose.sink.objectstorage.message.MessageProto;
import io.odpf.firehose.sink.objectstorage.message.Record;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TimePartitionPathTest {

    private final String zone = "UTC";
    private final String fieldName = MessageProto.CREATED_TIME_FIELD_NAME;
    private final String datePrefix = "dt=";
    private final String hourPrefix = "hr=";
    private final Instant defaultTimestamp = Instant.parse("2020-01-01T10:00:00.000Z");
    private final int defaultOrderNumber = 100;
    private final long defaultOffset = 1L;
    private final int defaultPartition = 1;
    private final String defaultTopic = "booking-log";
    private TimePartitionPath factory;

    @Test
    public void shouldCreateDayPartitioningPath() {
        Path partitionPath = Paths.get("booking-log/date=2020-01-01");
        String kafkaMetadataFieldName = "";

        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);

        factory = new TimePartitionPath(kafkaMetadataFieldName, fieldName, Constants.PartitioningType.DAY, zone, "date=", "");
        Path path = factory.create(record);

        assertEquals(partitionPath, path);
    }

    @Test
    public void shouldCreateHourPartitioningPath() {
        Path partitionPath = Paths.get("booking-log/dt=2020-01-01/hr=10");
        String kafkaMetadataFieldName = "";

        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);

        factory = new TimePartitionPath(kafkaMetadataFieldName, fieldName, Constants.PartitioningType.HOUR, zone, datePrefix, hourPrefix);
        Path path = factory.create(record);

        assertEquals(partitionPath, path);
    }

    @Test
    public void shouldCreateNonePartitioningPath() {
        Path partitionPath = Paths.get("booking-log");
        String kafkaMetadataFieldName = "";

        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);

        factory = new TimePartitionPath(kafkaMetadataFieldName, fieldName, Constants.PartitioningType.NONE, zone, datePrefix, hourPrefix);
        Path path = factory.create(record);

        assertEquals(partitionPath, path);
    }

    @Test
    public void shouldCreatePartitionPathWhenKafkaMetadataIsNotNested() {
        Path partitionPath = Paths.get("booking-log/dt=2020-01-01");
        String kafkaMetadataFieldName = "";

        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);

        factory = new TimePartitionPath(kafkaMetadataFieldName, fieldName, Constants.PartitioningType.DAY, zone, datePrefix, hourPrefix);
        Path path = factory.create(record);

        assertEquals(partitionPath, path);
    }

    @Test
    public void shouldCreatePartitioningPathForNestedKafkaMetadata() {
        Path partitionPath = Paths.get("booking-log/dt=2020-01-01");
        String kafkaMetadataFieldName = "meta";

        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);

        factory = new TimePartitionPath(kafkaMetadataFieldName, fieldName, Constants.PartitioningType.DAY, zone, datePrefix, hourPrefix);
        Path path = factory.create(record);

        assertEquals(partitionPath, path);
    }
}
