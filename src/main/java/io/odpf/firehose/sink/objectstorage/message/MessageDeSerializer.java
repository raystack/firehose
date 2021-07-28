package io.odpf.firehose.sink.objectstorage.message;

import com.gojek.de.stencil.parser.Parser;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.sink.exception.EmptyMessageException;
import io.odpf.firehose.sink.exception.UnknownFieldsException;
import lombok.AllArgsConstructor;

import static io.odpf.firehose.sink.common.ProtoUtil.isUnknownFieldExist;

@AllArgsConstructor
public class MessageDeSerializer {

    private final KafkaMetadataUtils metadataUtils;
    private final boolean doWriteKafkaMetadata;
    private final Parser protoParser;

    public Record deSerialize(Message message) throws DeserializerException {
        try {
            if (message.getLogMessage() == null || message.getLogMessage().length == 0) {
                throw new EmptyMessageException();
            }

            DynamicMessage logMessage = protoParser.parse(message.getLogMessage());

            if (isUnknownFieldExist(logMessage)) {
                throw new UnknownFieldsException(logMessage);
            }

            DynamicMessage kafkaMetadata = null;
            if (doWriteKafkaMetadata) {
                kafkaMetadata = metadataUtils.createKafkaMetadata(message);
            }
            return new Record(logMessage, kafkaMetadata);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException("failed to parse message", e);
        }
    }

}
