package com.gojek.esb.sink.http.client.deserializer;

import com.gojek.de.stencil.StencilClient;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.esb.aggregate.supply.AggregatedSupplyMessage;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.parser.ProtoParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.core.IsEqual.equalTo;

public class JsonDeserializerTest {
    private EsbMessage esbMessage;
    private ProtoParser protoParser;
    private StencilClient stencilClient;

    @Before
    public void setUp() {
        stencilClient = StencilClientFactory.getClient();
        protoParser = new ProtoParser(stencilClient, AggregatedSupplyMessage.class.getName());
    }

    @Test
    public void canDeserializeListOfEsbMessagesIntoJson() throws DeserializerException {
        JsonDeserializer deserializer = new JsonDeserializer(protoParser);

        String logKey = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigC";
        String logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";

        esbMessage = new EsbMessage(Base64.getDecoder().decode(logKey.getBytes()), Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);
        List<String> actualOutput = deserializer.deserialize(Collections.singletonList(esbMessage));
        Assert.assertThat(actualOutput, equalTo(Collections.singletonList("{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\",\"logKey\":\"{"
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\"}")));
    }

    @Test
    public void shouldDeserializeEvenWhenKeyMissing() throws DeserializerException {

        JsonDeserializer deserializer = new JsonDeserializer(protoParser);

        String logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";

        esbMessage = new EsbMessage(null, Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        List<String> actualOutput = deserializer.deserialize(Collections.singletonList(esbMessage));
        Assert.assertThat(actualOutput, equalTo(Collections.singletonList("{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\"}")));
    }

    @Test
    public void shouldDeserializeEvenWhenKeyIsEmpty() throws DeserializerException {

        JsonDeserializer deserializer = new JsonDeserializer(protoParser);

        String logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";

        esbMessage = new EsbMessage(new byte[]{}, Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        List<String> actualOutput = deserializer.deserialize(Collections.singletonList(esbMessage));
        Assert.assertThat(actualOutput, equalTo(Collections.singletonList("{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\"}")));
    }
    @Test
    public void shouldReturnJsonWrapperDeserialiserOnNullProtoClassName() {
        Deserializer deserializer = Deserializer.build(null);

        Assert.assertTrue(deserializer instanceof JsonWrapperDeserializer);
    }

    @Test
    public void shouldReturnJsonWrapperDeserialiserOnEmptyProtoClassName() {
        Deserializer deserializer = Deserializer.build("");

        Assert.assertTrue(deserializer instanceof JsonWrapperDeserializer);
    }
}
