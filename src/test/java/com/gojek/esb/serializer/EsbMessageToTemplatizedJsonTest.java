package com.gojek.esb.serializer;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.aggregate.supply.AggregatedSupplyMessage;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.exception.EglcConfigurationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.Base64;

import static org.mockito.MockitoAnnotations.initMocks;

public class EsbMessageToTemplatizedJsonTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private ProtoParser protoParser;

    private String logMessage;
    private String logKey;

    @Before
    public void setup() {
        initMocks(this);
        logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
        logKey = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigC";
    }

    @Test
    public void shouldProperlySerializeMessageToTemplateWithSingleUnknownField() {
        String template = "{\"test\":\"$.vehicle_type\"}";
        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = new ProtoParser(stencilClient, AggregatedSupplyMessage.class.getName());
        EsbMessageToTemplatizedJson esbMessageToTemplatizedJson = EsbMessageToTemplatizedJson
                .create(template, protoParser);
        EsbMessage esbMessage = new EsbMessage(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        String serializedMessage = esbMessageToTemplatizedJson.serialize(esbMessage);
        String expectedMessage = "{\"test\":\"BIKE\"}";
        Assert.assertEquals(expectedMessage, serializedMessage);
    }

    @Test
    public void shouldProperlySerializeMessageToTemplateWithAsItIs() {
        String template = "\"$._all_\"";
        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = new ProtoParser(stencilClient, AggregatedSupplyMessage.class.getName());
        EsbMessageToTemplatizedJson esbMessageToTemplatizedJson = EsbMessageToTemplatizedJson
                .create(template, protoParser);
        EsbMessage esbMessage = new EsbMessage(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        String serializedMessage = esbMessageToTemplatizedJson.serialize(esbMessage);
        String expectedMessage = "{\n"
                + "  \"window_start_time\": \"2017-03-20T10:54:00Z\",\n"
                + "  \"window_end_time\": \"2017-03-20T10:55:00Z\",\n"
                + "  \"s2_id_level\": 13,\n"
                + "  \"s2_id\": \"3344472187078705152\",\n"
                + "  \"vehicle_type\": \"BIKE\",\n"
                + "  \"unique_drivers\": \"3\"\n"
                + "}";
        Assert.assertEquals(expectedMessage, serializedMessage);
    }

    @Test
    public void shouldThrowIfNoPathsFoundInTheProto() {
        expectedException.expect(DeserializerException.class);
        expectedException.expectMessage("No results for path: $['invalidPath']");

        String template = "{\"test\":\"$.invalidPath\"}";
        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = new ProtoParser(stencilClient, AggregatedSupplyMessage.class.getName());
        EsbMessageToTemplatizedJson esbMessageToTemplatizedJson = EsbMessageToTemplatizedJson
                .create(template, protoParser);
        EsbMessage esbMessage = new EsbMessage(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        esbMessageToTemplatizedJson.serialize(esbMessage);
    }

    @Test
    public void shouldFailForNonJsonTemplate() {
        expectedException.expect(EglcConfigurationException.class);
        expectedException.expectMessage("must be a valid JSON.");

        String template = "{\"test:\"$.routes[0]\", \"$.order_number\" : \"xxx\"}";
        EsbMessageToTemplatizedJson.create(template, protoParser);
    }


    @Test
    public void shouldDoRegexMatchingToReplaceThingsFromProtobuf() {
        expectedException.expect(EglcConfigurationException.class);
        expectedException.expectMessage("must be a valid JSON.");

        String template = "{\"test:\"$.routes[0]\", \"$.order_number\" : \"xxx\"}";
        EsbMessageToTemplatizedJson.create(template, protoParser);
    }

    @Test
    public void shouldFailIfNoPathsFoundToReplace() {
        expectedException.expect(EglcConfigurationException.class);
        expectedException.expectMessage("No correct paths found in the template to be replaced from proto");

        String template = "{\"test\":\"invalidPath1\", \"invalidPath2\" : \"xxx\"}";
        EsbMessageToTemplatizedJson.create(template, protoParser);
    }
}
