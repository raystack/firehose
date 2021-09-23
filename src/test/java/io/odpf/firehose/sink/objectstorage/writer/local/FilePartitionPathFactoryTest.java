package io.odpf.firehose.sink.objectstorage.writer.local;

import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.sink.objectstorage.Constants;
import io.odpf.firehose.sink.objectstorage.TestUtils;
import io.odpf.firehose.sink.objectstorage.TestProtoMessage;
import io.odpf.firehose.sink.objectstorage.message.Record;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class FilePartitionPathFactoryTest {

    private final String zone = "UTC";
    private final String fieldName = TestProtoMessage.CREATED_TIME_FIELD_NAME;
    private final String datePrefix = "dt=";
    private final String hourPrefix = "hr=";
    private final Instant defaultTimestamp = Instant.parse("2020-01-01T10:00:00.000Z");
    private final int defaultOrderNumber = 100;
    private final long defaultOffset = 1L;
    private final int defaultPartition = 1;
    private final String defaultTopic = "booking-log";

    private FilePartitionPathFactory factory;

    @Test
    public void shouldCreateDayPartitioningPath() {
        Path partitionPath = Paths.get("booking-log/date=2020-01-01");
        String kafkaMetadataFieldName = "";

        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);

        factory = new FilePartitionPathFactory(kafkaMetadataFieldName, fieldName, new FilePartitionPathConfig(zone, Constants.FilePartitionType.DAY, "date=", ""));
        Path path = factory.getPartitionPath(record);

        assertEquals(partitionPath, path);
    }

    @Test
    public void shouldCreateHourPartitioningPath() {
        Path partitionPath = Paths.get("booking-log/dt=2020-01-01/hr=10");
        String kafkaMetadataFieldName = "";

        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);

        factory = new FilePartitionPathFactory(kafkaMetadataFieldName, fieldName, new FilePartitionPathConfig(zone, Constants.FilePartitionType.HOUR, datePrefix, hourPrefix));
        Path path = factory.getPartitionPath(record);

        assertEquals(partitionPath, path);
    }

    @Test
    public void shouldCreateNonePartitioningPath() {
        Path partitionPath = Paths.get("booking-log");
        String kafkaMetadataFieldName = "";

        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);

        factory = new FilePartitionPathFactory(kafkaMetadataFieldName, fieldName, new FilePartitionPathConfig(zone, Constants.FilePartitionType.NONE, datePrefix, hourPrefix));
        Path path = factory.getPartitionPath(record);

        assertEquals(partitionPath, path);
    }

    @Test
    public void shouldCreatePartitionPathWhenKafkaMetadataIsNotNested() {
        Path partitionPath = Paths.get("booking-log/dt=2020-01-01");
        String kafkaMetadataFieldName = "";

        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);

        factory = new FilePartitionPathFactory(kafkaMetadataFieldName, fieldName, new FilePartitionPathConfig(zone, Constants.FilePartitionType.DAY, datePrefix, hourPrefix));
        Path path = factory.getPartitionPath(record);

        assertEquals(partitionPath, path);
    }

    @Test
    public void shouldCreatePartitioningPathForNestedKafkaMetadata() {
        Path partitionPath = Paths.get("booking-log/dt=2020-01-01");
        String kafkaMetadataFieldName = "meta";

        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);

        factory = new FilePartitionPathFactory(kafkaMetadataFieldName, fieldName, new FilePartitionPathConfig(zone, Constants.FilePartitionType.DAY, datePrefix, hourPrefix));
        Path path = factory.getPartitionPath(record);

        assertEquals(partitionPath, path);
    }

    @Test
    public void shouldParsePathWithNoneAsPartition() {
        Path path = Paths.get("topic");
        String partitionPathString = "/topic";
        FilePartitionPath filePartitionPath = FilePartitionPath.parseFrom(partitionPathString, new FilePartitionPathConfig(zone, Constants.FilePartitionType.NONE, "", ""));
        Path result = filePartitionPath.getPath();

        assertEquals(path, result);
    }

    @Test
    public void shouldParseDayPartition() {
        Path path = Paths.get("topic", "dt=2021-01-01");
        String partitionPathString = "/topic/dt=2021-01-01";
        FilePartitionPath filePartitionPath = FilePartitionPath.parseFrom(partitionPathString, new FilePartitionPathConfig(zone, Constants.FilePartitionType.DAY, datePrefix, ""));
        Path result = filePartitionPath.getPath();
        assertEquals(path, result);
    }

    @Test
    public void shouldParseHourPartition() {
        Path path = Paths.get("topic", "dt=2021-01-01", "hr=03");
        String partitionPathString = "/topic/dt=2021-01-01/hr=03";
        FilePartitionPath filePartitionPath = FilePartitionPath.parseFrom(partitionPathString, new FilePartitionPathConfig(zone, Constants.FilePartitionType.HOUR, datePrefix, hourPrefix));
        Path result = filePartitionPath.getPath();
        assertEquals(path, result);
    }
}
