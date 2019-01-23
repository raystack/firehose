package com.gojek.esb.sink.db.field.message;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.feedback.FeedbackLogMessage;
import com.gojek.esb.feedback.Reason;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class DBCollectionFieldTest {

    private StencilClient stencilClient;

    @Before
    public void setUp() throws Exception {
        stencilClient = StencilClientFactory.getClient();
    }


    @Test
    public void shouldParseTheCollectionFieldAsString() throws Exception {

        Reason reason = Reason.newBuilder().setReasonId("1").setGroupId("1").build();
        Reason reason2 = Reason.newBuilder().setReasonId("2").setGroupId("2").build();
        ArrayList<Reason> reasons = new ArrayList<>();
        reasons.add(reason);
        reasons.add(reason2);
        FeedbackLogMessage feedback = FeedbackLogMessage
                .newBuilder()
                .addAllReason(reasons)
                .build();

        Descriptors.FieldDescriptor reasonFieldDescriptor = FeedbackLogMessage.getDescriptor().getFields().get(10);
        DynamicMessage feedbackParsed = new ProtoParser(stencilClient, "com.gojek.esb.feedback.FeedbackLogMessage").parse(feedback.toByteArray());
        Object columnValue = feedbackParsed.getField(reasonFieldDescriptor);

        DBCollectionField dbCollectionField = new DBCollectionField(columnValue, reasonFieldDescriptor);
        Object data = dbCollectionField.getColumn();

        Assert.assertEquals("[{\"reason_id\":\"1\",\"group_id\":\"1\"},{\"reason_id\":\"2\",\"group_id\":\"2\"}]", data);
    }

    @Test
    public void shouldBeAbleToParseCollectionFields() throws Exception {

        Reason reason = Reason.newBuilder().setReasonId("1").setGroupId("1").build();
        Reason reason2 = Reason.newBuilder().setReasonId("2").setGroupId("2").build();
        ArrayList<Reason> reasons = new ArrayList<>();
        reasons.add(reason);
        reasons.add(reason2);
        FeedbackLogMessage feedback = FeedbackLogMessage
                .newBuilder()
                .addAllReason(reasons)
                .build();

        Descriptors.FieldDescriptor reasonFieldDescriptor = FeedbackLogMessage.getDescriptor().getFields().get(10);
        DynamicMessage feedbackParsed = new ProtoParser(stencilClient, "com.gojek.esb.feedback.FeedbackLogMessage").parse(feedback.toByteArray());
        Object columnValue = feedbackParsed.getField(reasonFieldDescriptor);

        DBCollectionField dbCollectionField = new DBCollectionField(columnValue, reasonFieldDescriptor);

        Assert.assertTrue("Should be able to process collection Fields", dbCollectionField.canProcess());
    }

    @Test
    public void shouldNotBeAbleToParseStringFields() throws Exception {

        Reason reason = Reason.newBuilder().setReasonId("1").setGroupId("1").build();
        Reason reason2 = Reason.newBuilder().setReasonId("2").setGroupId("2").build();
        ArrayList<Reason> reasons = new ArrayList<>();
        reasons.add(reason);
        reasons.add(reason2);
        FeedbackLogMessage feedback = FeedbackLogMessage
                .newBuilder()
                .setOrderNumber("order_number")
                .addAllReason(reasons)
                .build();

        Descriptors.FieldDescriptor orderNumberFieldDescriptor = FeedbackLogMessage.getDescriptor().getFields().get(0);
        DynamicMessage feedbackParsed = new ProtoParser(stencilClient, "com.gojek.esb.feedback.FeedbackLogMessage").parse(feedback.toByteArray());
        Object columnValue = feedbackParsed.getField(orderNumberFieldDescriptor);

        DBCollectionField dbCollectionField = new DBCollectionField(columnValue, orderNumberFieldDescriptor);

        Assert.assertFalse("Should not be able to process repeated Fields", dbCollectionField.canProcess());
    }


}
