package io.odpf.firehose.sink.objectstorage.writer.local;

import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.config.ObjectStorageSinkConfig;
import io.odpf.firehose.sink.objectstorage.Constants;
import io.odpf.firehose.sink.objectstorage.TestUtils;
import io.odpf.firehose.sink.objectstorage.TestProtoMessage;
import io.odpf.firehose.sink.objectstorage.message.Record;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class FilePartitionPathUtilTest {

    private final String zone = "UTC";
    private final String timeStampFieldName = TestProtoMessage.CREATED_TIME_FIELD_NAME;
    private final String datePrefix = "dt=";
    private final String hourPrefix = "hr=";
    private final Instant defaultTimestamp = Instant.parse("2020-01-01T10:00:00.000Z");
    private final int defaultOrderNumber = 100;
    private final long defaultOffset = 1L;
    private final int defaultPartition = 1;
    private final String defaultTopic = "booking-log";

    @Test
    public void shouldCreateDayPartitioningPath() {
        String kafkaMetadataFieldName = "";
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        ObjectStorageSinkConfig sinkConfig = Mockito.mock(ObjectStorageSinkConfig.class);
        Mockito.when(sinkConfig.getTimePartitioningTimeZone()).thenReturn(zone);
        Mockito.when(sinkConfig.getTimePartitioningFieldName()).thenReturn(timeStampFieldName);
        Mockito.when(sinkConfig.getPartitioningType()).thenReturn(Constants.FilePartitionType.DAY);
        Mockito.when(sinkConfig.getKafkaMetadataColumnName()).thenReturn(kafkaMetadataFieldName);
        Mockito.when(sinkConfig.getTimePartitioningDatePrefix()).thenReturn("date=");
        Mockito.when(sinkConfig.getTimePartitioningHourPrefix()).thenReturn("");
        Path path = FilePartitionPathUtil.getFilePartitionPath(record, sinkConfig);
        assertEquals(Paths.get("booking-log/date=2020-01-01"), path);
    }

    @Test
    public void shouldCreateHourPartitioningPath() {
        String kafkaMetadataFieldName = "";
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        ObjectStorageSinkConfig sinkConfig = Mockito.mock(ObjectStorageSinkConfig.class);
        Mockito.when(sinkConfig.getTimePartitioningTimeZone()).thenReturn(zone);
        Mockito.when(sinkConfig.getKafkaMetadataColumnName()).thenReturn(kafkaMetadataFieldName);
        Mockito.when(sinkConfig.getTimePartitioningFieldName()).thenReturn(timeStampFieldName);
        Mockito.when(sinkConfig.getPartitioningType()).thenReturn(Constants.FilePartitionType.HOUR);
        Mockito.when(sinkConfig.getTimePartitioningDatePrefix()).thenReturn(datePrefix);
        Mockito.when(sinkConfig.getTimePartitioningHourPrefix()).thenReturn(hourPrefix);
        Path path = FilePartitionPathUtil.getFilePartitionPath(record, sinkConfig);
        assertEquals(Paths.get("booking-log/dt=2020-01-01/hr=10"), path);
    }

    @Test
    public void shouldCreateNonePartitioningPath() {
        String kafkaMetadataFieldName = "";
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        ObjectStorageSinkConfig sinkConfig = Mockito.mock(ObjectStorageSinkConfig.class);
        Mockito.when(sinkConfig.getTimePartitioningTimeZone()).thenReturn(zone);
        Mockito.when(sinkConfig.getKafkaMetadataColumnName()).thenReturn(kafkaMetadataFieldName);
        Mockito.when(sinkConfig.getTimePartitioningFieldName()).thenReturn(timeStampFieldName);
        Mockito.when(sinkConfig.getPartitioningType()).thenReturn(Constants.FilePartitionType.NONE);
        Mockito.when(sinkConfig.getTimePartitioningDatePrefix()).thenReturn(datePrefix);
        Mockito.when(sinkConfig.getTimePartitioningHourPrefix()).thenReturn(hourPrefix);
        Path path = FilePartitionPathUtil.getFilePartitionPath(record, sinkConfig);
        assertEquals(Paths.get("booking-log"), path);
    }

    @Test
    public void shouldCreatePartitionPathWhenKafkaMetadataIsNotNested() {
        String kafkaMetadataFieldName = "";
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        ObjectStorageSinkConfig sinkConfig = Mockito.mock(ObjectStorageSinkConfig.class);
        Mockito.when(sinkConfig.getTimePartitioningTimeZone()).thenReturn(zone);
        Mockito.when(sinkConfig.getPartitioningType()).thenReturn(Constants.FilePartitionType.DAY);
        Mockito.when(sinkConfig.getTimePartitioningFieldName()).thenReturn(timeStampFieldName);
        Mockito.when(sinkConfig.getTimePartitioningDatePrefix()).thenReturn(datePrefix);
        Mockito.when(sinkConfig.getTimePartitioningHourPrefix()).thenReturn(hourPrefix);
        Mockito.when(sinkConfig.getKafkaMetadataColumnName()).thenReturn(kafkaMetadataFieldName);
        Path path = FilePartitionPathUtil.getFilePartitionPath(record, sinkConfig);
        assertEquals(Paths.get("booking-log/dt=2020-01-01"), path);
    }

    @Test
    public void shouldCreatePartitioningPathForNestedKafkaMetadata() {
        String kafkaMetadataFieldName = "meta";
        DynamicMessage message = TestUtils.createMessage(defaultTimestamp, defaultOrderNumber);
        DynamicMessage metadata = TestUtils.createMetadata(kafkaMetadataFieldName, defaultTimestamp, defaultOffset, defaultPartition, defaultTopic);
        Record record = new Record(message, metadata);
        ObjectStorageSinkConfig sinkConfig = Mockito.mock(ObjectStorageSinkConfig.class);
        Mockito.when(sinkConfig.getTimePartitioningTimeZone()).thenReturn(zone);
        Mockito.when(sinkConfig.getPartitioningType()).thenReturn(Constants.FilePartitionType.DAY);
        Mockito.when(sinkConfig.getTimePartitioningFieldName()).thenReturn(timeStampFieldName);
        Mockito.when(sinkConfig.getTimePartitioningDatePrefix()).thenReturn(datePrefix);
        Mockito.when(sinkConfig.getTimePartitioningHourPrefix()).thenReturn(hourPrefix);
        Mockito.when(sinkConfig.getKafkaMetadataColumnName()).thenReturn(kafkaMetadataFieldName);
        Path path = FilePartitionPathUtil.getFilePartitionPath(record, sinkConfig);
        assertEquals(Paths.get("booking-log/dt=2020-01-01"), path);
    }
}
