package io.odpf.firehose.sinkdecorator.dlq.objectstorage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.objectstorage.ObjectStorageException;
import io.odpf.firehose.objectstorage.ObjectStorage;
import io.odpf.firehose.sinkdecorator.dlq.DlqWriter;
import lombok.extern.slf4j.Slf4j;
import org.bson.internal.Base64;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class ObjectStorageDlqWriter implements DlqWriter {
    private final ObjectStorage objectStorage;
    private final ObjectMapper objectMapper;

    public ObjectStorageDlqWriter(ObjectStorage objectStorage) {
        this.objectStorage = objectStorage;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public List<Message> write(List<Message> messages) throws IOException {
        Map<Path, List<Message>> messagesByPartition = messages.stream().collect(Collectors.groupingBy(this::createPartition));
        List<Message> failedMessages = new LinkedList<>();
        messagesByPartition.forEach((path, partitionedMessages) -> {
            String data = partitionedMessages.stream().map(this::convertToString).collect(Collectors.joining("\n"));
            String fileName = UUID.randomUUID().toString();
            String objectName = path.resolve(fileName).toString();
            try {
                objectStorage.store(objectName, data.getBytes(StandardCharsets.UTF_8));
            } catch (ObjectStorageException e) {
                log.warn("Not able to store into DLQ messages into object storage", e);
                failedMessages.addAll(partitionedMessages);
            }
        });
        return failedMessages;
    }

    private String convertToString(Message message) {
        try {
            return objectMapper.writeValueAsString(new DlqMessage(
                    Base64.encode(message.getLogKey()),
                    Base64.encode(message.getLogMessage()),
                    message.getTopic(),
                    message.getPartition(),
                    message.getOffset(),
                    message.getTimestamp(),
                    message.getErrorInfo().toString()));
        } catch (JsonProcessingException e) {
            log.warn("Not able to convert message into json", e);
            return "";
        }
    }

    private Path createPartition(Message message) {
        LocalDate consumeLocalDate = LocalDate.from(Instant.ofEpochMilli(message.getConsumeTimestamp())
                .atZone(ZoneId.of("UTC")));
        String consumeDate = DateTimeFormatter.ISO_LOCAL_DATE.format(consumeLocalDate);
        return Paths.get(message.getTopic(), consumeDate);
    }
}
