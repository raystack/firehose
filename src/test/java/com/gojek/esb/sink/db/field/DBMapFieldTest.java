package com.gojek.esb.sink.db.field;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.consumer.TestAuditEntityLogMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

public class DBMapFieldTest {


    private StencilClient stencilClient;

    @Before
    public void setUp() throws Exception {
        stencilClient = StencilClientFactory.getClient();
    }

    @Test
    public void shouldParseTheMapFieldAsString() throws Exception {


        HashMap<String, String> currentStates = new HashMap<>();
        currentStates.put("key", "value");
        currentStates.put("key2", "value2");
        TestAuditEntityLogMessage auditEntityLogMessage = TestAuditEntityLogMessage.newBuilder().putAllCurrentState(currentStates).build();

        Descriptors.FieldDescriptor currentEntityFieldDescriptor = TestAuditEntityLogMessage.getDescriptor().getFields().get(6);
        DynamicMessage auditEntityParsed = new ProtoParser(stencilClient, "com.gojek.esb.consumer.TestAuditEntityLogMessage").parse(auditEntityLogMessage.toByteArray());
        Object columnValue = auditEntityParsed.getField(currentEntityFieldDescriptor);

        DBMapField dbMapField = new DBMapField(columnValue, currentEntityFieldDescriptor);

        Object data = dbMapField.getColumn();

        Assert.assertEquals("{\"key2\":\"value2\",\"key\":\"value\"}", data);
    }

    @Test
    public void shouldParseTheMapFieldWithEmptyValueAsString() throws Exception {
        HashMap<String, String> currentStates = new HashMap<>();
        currentStates.put("key", "value");
        currentStates.put("key2", "value2");
        currentStates.put("key3", "");
        TestAuditEntityLogMessage auditEntityLogMessage = TestAuditEntityLogMessage.newBuilder().putAllCurrentState(currentStates).build();

        Descriptors.FieldDescriptor currentEntityFieldDescriptor = TestAuditEntityLogMessage.getDescriptor().getFields().get(6);
        DynamicMessage auditEntityParsed = new ProtoParser(stencilClient, "com.gojek.esb.consumer.TestAuditEntityLogMessage").parse(auditEntityLogMessage.toByteArray());
        Object columnValue = auditEntityParsed.getField(currentEntityFieldDescriptor);

        DBMapField dbMapField = new DBMapField(columnValue, currentEntityFieldDescriptor);

        Object data = dbMapField.getColumn();

        Assert.assertEquals("{\"key2\":\"value2\",\"key3\":\"\",\"key\":\"value\"}", data);
    }

    @Test
    public void shouldBeAbleToParseMapFields() throws Exception {

        HashMap<String, String> currentStates = new HashMap<>();
        currentStates.put("key", "value");
        currentStates.put("key2", "value2");
        currentStates.put("key3", "value3");
        TestAuditEntityLogMessage auditEntityLogMessage = TestAuditEntityLogMessage.newBuilder().putAllCurrentState(currentStates).build();

        Descriptors.FieldDescriptor currentEntityFieldDescriptor = TestAuditEntityLogMessage.getDescriptor().getFields().get(6);
        DynamicMessage auditEntityParsed = new ProtoParser(stencilClient, "com.gojek.esb.consumer.TestAuditEntityLogMessage").parse(auditEntityLogMessage.toByteArray());
        Object columnValue = auditEntityParsed.getField(currentEntityFieldDescriptor);

        DBMapField dbMapField = new DBMapField(columnValue, currentEntityFieldDescriptor);

        Assert.assertTrue("Should be able to process map Fields", dbMapField.canProcess());
    }

    @Test
    public void shouldBeAbleToParseStringFields() throws Exception {

        HashMap<String, String> currentStates = new HashMap<>();
        currentStates.put("key", "value");
        currentStates.put("key2", "value2");
        TestAuditEntityLogMessage auditEntityLogMessage = TestAuditEntityLogMessage.newBuilder().setAuditId("audit_id").putAllCurrentState(currentStates).build();

        Descriptors.FieldDescriptor auditIdFieldDescriptor = TestAuditEntityLogMessage.getDescriptor().getFields().get(0);
        DynamicMessage auditEntityParsed = new ProtoParser(stencilClient, "com.gojek.esb.consumer.TestAuditEntityLogMessage").parse(auditEntityLogMessage.toByteArray());
        Object columnValue = auditEntityParsed.getField(auditIdFieldDescriptor);

        DBMapField dbMapField = new DBMapField(columnValue, auditIdFieldDescriptor);

        Assert.assertFalse("Should not be able to process repeated Fields", dbMapField.canProcess());
    }


}
