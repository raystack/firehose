package com.gojek.esb.sink.db.field.message;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.types.Location;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DBDefaultMessageFieldTest {
    private StencilClient stencilClient;

    @Before
    public void setUp() throws Exception {
        stencilClient = StencilClientFactory.getClient();
    }

    @Test
    public void shouldParseTheMessageFieldAsString() throws Exception {

        BookingLogMessage booking = BookingLogMessage.newBuilder().setDriverDropoffLocation(Location.newBuilder().setName("location_address").build()).build();

        Descriptors.FieldDescriptor locationFieldDescriptor = BookingLogMessage.getDescriptor().getFields().get(25);
        DynamicMessage feedbackParsed = new ProtoParser(stencilClient, "com.gojek.esb.booking.BookingLogMessage").parse(booking.toByteArray());
        Object columnValue = feedbackParsed.getField(locationFieldDescriptor);

        DBDefaultMessageField dbDefaultMessageField = new DBDefaultMessageField(columnValue);
        Assert.assertEquals("{\"name\":\"\",\"address\":\"\",\"latitude\":0.0,\"longitude\":0.0,\"type\":\"\",\"note\":\"\",\"place_id\":\"\",\"accuracy_meter\":0.0,\"gate_id\":\"\"}", dbDefaultMessageField.getColumn());
    }

    @Test
    public void shouldBeAbleToParseMessageFields() throws Exception {

        BookingLogMessage booking = BookingLogMessage.newBuilder().setDriverDropoffLocation(Location.newBuilder().setName("location_address").build()).build();

        Descriptors.FieldDescriptor locationFieldDescriptor = BookingLogMessage.getDescriptor().getFields().get(25);
        DynamicMessage feedbackParsed = new ProtoParser(stencilClient, "com.gojek.esb.booking.BookingLogMessage").parse(booking.toByteArray());
        Object columnValue = feedbackParsed.getField(locationFieldDescriptor);

        DBDefaultMessageField dbDefaultMessageField = new DBDefaultMessageField(columnValue);

        Assert.assertTrue("Should be able to process default message Fields", dbDefaultMessageField.canProcess());
    }

    @Test
    public void shouldNotBeAbleToParseNormalFields() throws Exception {

        BookingLogMessage booking = BookingLogMessage.newBuilder().setDriverDropoffLocation(Location.newBuilder().setName("location_address").build()).setCustomerId("customer_id").build();

        Descriptors.FieldDescriptor customerIdFieldDescriptor = BookingLogMessage.getDescriptor().getFields().get(5);
        DynamicMessage feedbackParsed = new ProtoParser(stencilClient, "com.gojek.esb.booking.BookingLogMessage").parse(booking.toByteArray());
        Object columnValue = feedbackParsed.getField(customerIdFieldDescriptor);

        DBDefaultMessageField dbDefaultMessageField = new DBDefaultMessageField(columnValue);

        Assert.assertFalse("Should not be able to process repeated Fields", dbDefaultMessageField.canProcess());
    }


}
