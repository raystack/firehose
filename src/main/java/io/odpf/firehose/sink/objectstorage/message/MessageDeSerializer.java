package io.odpf.firehose.sink.objectstorage.message;

import com.gojek.de.stencil.parser.Parser;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.sink.exception.EmptyMessageException;
import io.odpf.firehose.sink.exception.UnknownFieldsException;
import lombok.AllArgsConstructor;

import static io.odpf.firehose.sink.common.ProtoUtils.hasUnknownField;

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
            DynamicMessage dynamicMessage = protoParser.parse(message.getLogMessage());

            if (hasUnknownField(dynamicMessage)) {
                throw new UnknownFieldsException(dynamicMessage);
            }

            DynamicMessage kafkaMetadata = null;
            if (doWriteKafkaMetadata) {
                kafkaMetadata = metadataUtils.createKafkaMetadata(message);
            }
            return new Record(dynamicMessage, kafkaMetadata);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException("failed to parse message", e);
        }
    }

}
