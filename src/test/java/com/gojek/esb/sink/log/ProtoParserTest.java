package com.gojek.esb.sink.log;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.esb.consumer.TestMessage;
import com.gojek.esb.feedback.FeedbackLogMessage;
import com.gojek.esb.parser.EsbLogConsumerConfigException;
import com.gojek.esb.parser.ProtoParser;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.gradle.internal.impldep.org.testng.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.gradle.internal.impldep.org.testng.Assert.assertEquals;
import static org.gradle.internal.impldep.org.testng.Assert.assertNotEquals;

@RunWith(MockitoJUnitRunner.class)
public class ProtoParserTest {

    private ProtoParser testMessageParser;
    private TestMessage testMessage;
    private StencilClient stencilClient;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setup() {
        stencilClient = StencilClientFactory.getClient();
        testMessageParser = new ProtoParser(stencilClient, TestMessage.class.getName());
        testMessage = TestMessage.newBuilder()
                .setOrderNumber("order")
                .build();
    }

    @Test
    public void shouldParseTestMessage() throws InvalidProtocolBufferException {
        DynamicMessage dynamicMessage = testMessageParser.parse(testMessage.toByteArray());

        Descriptors.FieldDescriptor fieldDescriptor = dynamicMessage.getDescriptorForType().getFields().get(0);

        assertEquals(dynamicMessage.toByteArray(), testMessage.toByteArray());
        assertEquals(dynamicMessage.getField(fieldDescriptor), "order");
    }

    @Test
    public void shouldNotParseFeedbackLogMessage() throws InvalidProtocolBufferException {
        FeedbackLogMessage feedbackLogMessage = FeedbackLogMessage.newBuilder()
                .setFeedbackComment("comment")
                .setFeedbackRating(4)
                .setCustomerId("customer1")
                .build();

        DynamicMessage message = testMessageParser.parse(feedbackLogMessage.toByteArray());

        assertNotEquals(message.toByteArray(), feedbackLogMessage.toByteArray());
        assertEquals(message.getAllFields().size(), 0);
    }

    @Test
    public void shouldFailWhenNotAbleToFindTheProtoClass() throws Exception {
        exception.expect(EsbLogConsumerConfigException.class);
        exception.expectMessage("No Descriptors found for invalid_class_name");
        ProtoParser protoParser = new ProtoParser(stencilClient, "invalid_class_name");

        protoParser.parse(testMessage.toByteArray());

        Assert.fail("Expected to get an exception");
    }
}
