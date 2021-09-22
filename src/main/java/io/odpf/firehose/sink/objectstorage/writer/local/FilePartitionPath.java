package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.sink.objectstorage.Constants;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;


/**
 * FilePartitionPath contains formatted path string topic/dt=yyyy-MM-dd/hr=HH .
 */
@AllArgsConstructor
@Data
public class FilePartitionPath {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH");

    private String topic;
    private Instant timestamp;
    private FilePartitionPathConfig filePartitionPathConfig;

    private String getDate() {
        LocalDate localDate = LocalDateTime.ofInstant(timestamp, ZoneId.of(filePartitionPathConfig.getZone())).toLocalDate();
        return DATE_FORMATTER.format(localDate);
    }

    private String getHour() {
        LocalTime localTime = LocalDateTime.ofInstant(timestamp, ZoneId.of(filePartitionPathConfig.getZone())).toLocalTime();
        return TIME_FORMATTER.format(localTime);
    }


    public String getDatetimePathWithoutPrefix() {
        String datePart = getDate();
        String hourPart = getHour();
        return getPath(datePart, hourPart);
    }

    private String getPath(String datePart, String hourPart) {
        switch (filePartitionPathConfig.getFilePartitionType()) {
            case DAY:
                return String.format("%s", datePart);
            case HOUR:
                return String.format("%s/%s", datePart, hourPart);
            default:
                throw new IllegalArgumentException();
        }
    }

    private String getDatetimePath() {
        String datePart = getDate();
        String dateSegment = String.format("%s%s", filePartitionPathConfig.getDatePrefix(), datePart);

        String hourPart = getHour();
        String hourSegment = String.format("%s%s", filePartitionPathConfig.getHourPrefix(), hourPart);

        return getPath(dateSegment, hourSegment);
    }


    private String getPartitionPath() {
        if (filePartitionPathConfig.getFilePartitionType() == Constants.FilePartitionType.NONE) {
            return topic;
        }

        String datetimePartition = getDatetimePath();
        return String.format("%s/%s", topic, datetimePartition);
    }

    /**
     * Create PartitionPath object from partition path.
     *
     * @param filePartitionPath       partition path is a relative path, it should only contains partition segment, it should not contains file name or base directory path
     * @param filePartitionPathConfig
     * @return
     */
    public static FilePartitionPath parseFrom(String filePartitionPath, FilePartitionPathConfig filePartitionPathConfig) {
        Path path = Paths.get(filePartitionPath);
        String topic = path.getName(0).toString();
        if (filePartitionPathConfig.getFilePartitionType() == Constants.FilePartitionType.NONE) {
            return new FilePartitionPath(topic, null, filePartitionPathConfig);
        }

        String datePath = path.getName(1).toString().replace(filePartitionPathConfig.getDatePrefix(), "");
        ZonedDateTime zonedDateTime = LocalDate.parse(datePath, DATE_FORMATTER).atStartOfDay(ZoneId.of(filePartitionPathConfig.getZone()));
        if (filePartitionPathConfig.getFilePartitionType() == Constants.FilePartitionType.DAY) {
            return new FilePartitionPath(topic, zonedDateTime.toInstant(), filePartitionPathConfig);
        }

        String timePath = path.getName(2).toString().replace(filePartitionPathConfig.getHourPrefix(), "");
        LocalTime localTime = LocalTime.parse(timePath, TIME_FORMATTER);
        zonedDateTime = zonedDateTime.plusHours(localTime.getHour());
        if (filePartitionPathConfig.getFilePartitionType() == Constants.FilePartitionType.HOUR) {
            return new FilePartitionPath(topic, zonedDateTime.toInstant(), filePartitionPathConfig);
        }

        throw new IllegalArgumentException();
    }

    public Path getPath() {
        return Paths.get(getPartitionPath());
    }

    @Override
    public String toString() {
        return getPartitionPath();
    }
}
