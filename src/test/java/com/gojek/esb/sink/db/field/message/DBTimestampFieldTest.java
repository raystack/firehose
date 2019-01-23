package com.gojek.esb.sink.db.field.message;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.gofood.AuditEntityLogMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;

public class DBTimestampFieldTest {

    private StencilClient stencilClient;

    @Before
    public void setUp() throws Exception {
        stencilClient = StencilClientFactory.getClient();
    }

    @Test
    public void shouldParseTheTimestampFieldAsString() throws Exception {

        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        AuditEntityLogMessage auditEntityLogMessage = AuditEntityLogMessage.newBuilder().setEventTimestamp(timestamp).build();

        Descriptors.FieldDescriptor timestampFieldDescriptor = AuditEntityLogMessage.getDescriptor().getFields().get(3);
        DynamicMessage auditEntityParsed = new ProtoParser(stencilClient, "com.gojek.esb.gofood.AuditEntityLogMessage").parse(auditEntityLogMessage.toByteArray());
        Object columnValue = auditEntityParsed.getField(timestampFieldDescriptor);

        DBTimestampField dbTime = new DBTimestampField(columnValue);

        Assert.assertEquals(now, dbTime.getColumn());
    }

    @Test
    public void shouldBeAbleToParseTimestampFields() throws Exception {

        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        AuditEntityLogMessage auditEntityLogMessage = AuditEntityLogMessage.newBuilder().setEventTimestamp(timestamp).build();

        Descriptors.FieldDescriptor timestampFieldDescriptor = AuditEntityLogMessage.getDescriptor().getFields().get(3);
        DynamicMessage auditEntityParsed = new ProtoParser(stencilClient, "com.gojek.esb.gofood.AuditEntityLogMessage").parse(auditEntityLogMessage.toByteArray());
        Object columnValue = auditEntityParsed.getField(timestampFieldDescriptor);

        DBTimestampField dbTime = new DBTimestampField(columnValue);

        Assert.assertTrue("Should be able to process timestamp Fields", dbTime.canProcess());
    }

    @Test
    public void shouldNotBeAbleToParseStringFields() throws Exception {

        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        AuditEntityLogMessage auditEntityLogMessage = AuditEntityLogMessage.newBuilder().setEventTimestamp(timestamp).setAuditId("audit_id").build();

        Descriptors.FieldDescriptor auditIdFieldDescriptor = AuditEntityLogMessage.getDescriptor().getFields().get(0);
        DynamicMessage auditEntityParsed = new ProtoParser(stencilClient, "com.gojek.esb.gofood.AuditEntityLogMessage").parse(auditEntityLogMessage.toByteArray());
        Object columnValue = auditEntityParsed.getField(auditIdFieldDescriptor);

        DBTimestampField dbTime = new DBTimestampField(columnValue);

        Assert.assertFalse("Should not be able to process string Fields", dbTime.canProcess());
    }

}
