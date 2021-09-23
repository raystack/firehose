package io.odpf.firehose.sink.objectstorage.proto;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.protobuf.Descriptors;
import io.odpf.firehose.sink.objectstorage.TestProtoMessage;
import org.junit.Test;

import static org.junit.Assert.*;

public class KafkaMetadataProtoUtilsTest {

    @Test
    public void shouldCreateDescriptors() {
        String kafkaMetadataColumnName = "";
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoUtils.createFileDescriptor(kafkaMetadataColumnName);
        Descriptors.Descriptor kafkaMetadataDescriptor = fileDescriptor.findMessageTypeByName(KafkaMetadataMessage.getTypeName());
        Descriptors.Descriptor timestampDescriptor = fileDescriptor.findMessageTypeByName(TimestampMetadataMessage.getTypeName());

        assertNotNull(kafkaMetadataDescriptor);
        assertNotNull(timestampDescriptor);
    }

    @Test
    public void shouldCreateDescriptorsForNestedMetadata() {
        String kafkaMetadataColumnName = "metadata_column_name";
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoUtils.createFileDescriptor(kafkaMetadataColumnName);
        Descriptors.Descriptor kafkaMetadataDescriptor = fileDescriptor.findMessageTypeByName(KafkaMetadataMessage.getTypeName());
        Descriptors.Descriptor timestampDescriptor = fileDescriptor.findMessageTypeByName(TimestampMetadataMessage.getTypeName());
        Descriptors.Descriptor nestedKafkaMetadataDescriptor = fileDescriptor.findMessageTypeByName(NestedKafkaMetadataMessage.getTypeName());

        assertNotNull(kafkaMetadataDescriptor);
        assertNotNull(timestampDescriptor);
        assertNotNull(nestedKafkaMetadataDescriptor);
    }


    @Test
    public void shouldCreateFileDescriptor() {
        DynamicSchema dynamicSchema = TestProtoMessage.createSchema();
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoUtils.createFileDescriptor(dynamicSchema);
        Descriptors.Descriptor descriptor = fileDescriptor.findMessageTypeByName(TestProtoMessage.getTypeName());

        assertNotNull(descriptor);
        assertNotNull(descriptor.findFieldByName(TestProtoMessage.ORDER_NUMBER_FIELD_NAME));
        assertNotNull(descriptor.findFieldByName(TestProtoMessage.CREATED_TIME_FIELD_NAME));
    }
}
