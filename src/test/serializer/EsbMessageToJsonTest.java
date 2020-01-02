package com.gojek.esb.serializer;

import static org.junit.Assert.assertEquals;

import java.util.Base64;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.aggregate.supply.AggregatedSupplyMessage;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;

import org.junit.Before;
import org.junit.Test;

public class EsbMessageToJsonTest {
  String logMessage;
  String logKey;
  private ProtoParser protoParser;

  @Before
  public void setUp() {
    StencilClient stencilClient = StencilClientFactory.getClient();
    protoParser = new ProtoParser(stencilClient, AggregatedSupplyMessage.class.getName());
    logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
    logKey = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigC";
  }

  @Test
  public void shouldProperlySerializeEsbMessage() throws DeserializerException {
    EsbMessageToJson esbMessageToJson = new EsbMessageToJson(protoParser, false);

    EsbMessage esbMessage = new EsbMessage(Base64.getDecoder().decode(logKey.getBytes()),
        Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);
    String actualOutput = esbMessageToJson.serialize(esbMessage);
    assertEquals(actualOutput, "{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
        + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
        + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
        + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\",\"logKey\":\"{"
        + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
        + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
        + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\"}");
  }

  @Test
  public void shouldSerializeWhenKeyIsMissing() throws DeserializerException {
    EsbMessageToJson esbMessageToJson = new EsbMessageToJson(protoParser, false);

    EsbMessage esbMessage = new EsbMessage(null, Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0,
        100);
    String actualOutput = esbMessageToJson.serialize(esbMessage);
    assertEquals("{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
        + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
        + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
        + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\"}", actualOutput);
  }

  @Test
  public void shouldSerializeWhenKeyIsEmpty() throws DeserializerException {
    EsbMessageToJson esbMessageToJson = new EsbMessageToJson(protoParser, false);

    EsbMessage esbMessage = new EsbMessage(new byte[] {}, Base64.getDecoder().decode(logMessage.getBytes()),
        "sample-topic", 0, 100);

    String actualOutput = esbMessageToJson.serialize(esbMessage);
    assertEquals("{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
        + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
        + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
        + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\"}", actualOutput);
  }
}
