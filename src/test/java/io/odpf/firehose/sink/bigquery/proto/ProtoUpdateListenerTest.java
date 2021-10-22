package io.odpf.firehose.sink.bigquery.proto;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.models.DescriptorAndTypeName;
import com.gojek.de.stencil.parser.ProtoParser;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import io.odpf.firehose.TestKeyBQ;
import io.odpf.firehose.config.BigQuerySinkConfig;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.sink.bigquery.converter.MessageRecordConverterCache;
import io.odpf.firehose.sink.bigquery.handler.BigQueryClient;
import io.odpf.firehose.sink.bigquery.models.MetadataUtil;
import io.odpf.firehose.sink.bigquery.models.ProtoField;
import io.odpf.firehose.sink.bigquery.models.Records;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProtoUpdateListenerTest {
    @Mock
    private BigQueryClient bigQueryClient;
    @Mock
    private StencilClient stencilClient;

    private BigQuerySinkConfig config;

    private MessageRecordConverterCache converterWrapper;

    @Before
    public void setUp() {
        System.setProperty("INPUT_SCHEMA_PROTO_CLASS", "io.odpf.firehose.TestKeyBQ");
        System.setProperty("SINK_BIGQUERY_ENABLE_AUTO_SCHEMA_UPDATE", "false");
        config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        converterWrapper = new MessageRecordConverterCache();
    }

    @Test
    public void shouldUseNewSchemaIfProtoChanges() throws IOException {
        ProtoUpdateListener protoUpdateListener = new ProtoUpdateListener(config, bigQueryClient, converterWrapper);

        ProtoField returnedProtoField = new ProtoField();
        returnedProtoField.addField(ProtoUtil.createProtoField("order_number", 1));
        returnedProtoField.addField(ProtoUtil.createProtoField("order_url", 2));

        HashMap<String, DescriptorAndTypeName> descriptorsMap = new HashMap<String, DescriptorAndTypeName>() {{
            put(String.format("%s", TestKeyBQ.class.getName()), new DescriptorAndTypeName(TestKeyBQ.getDescriptor(), String.format(".%s.%s", TestKeyBQ.getDescriptor().getFile().getPackage(), TestKeyBQ.getDescriptor().getName())));
        }};
        when(stencilClient.get(TestKeyBQ.class.getName())).thenReturn(descriptorsMap.get(TestKeyBQ.class.getName()).getDescriptor());
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            addAll(MetadataUtil.getMetadataFields());
        }};
        doNothing().when(bigQueryClient).upsertTable(bqSchemaFields);

        ProtoParser protoParser = new ProtoParser(stencilClient, config.getInputSchemaProtoClass());
        protoUpdateListener.setStencilParser(protoParser);
        protoUpdateListener.onProtoUpdate("", descriptorsMap);
        TestKeyBQ testKeyBQ = TestKeyBQ.newBuilder().setOrderNumber("order").setOrderUrl("test").build();
        Instant now = Instant.now();
        Message testMessage = new Message("".getBytes(), testKeyBQ.toByteArray(), "topic", 1, 1);
        Records convert = protoUpdateListener.getMessageRecordConverterCache().getMessageRecordConverter().convert(Collections.singletonList(testMessage), now);
        Assert.assertEquals(1, convert.getValidRecords().size());
        Assert.assertEquals("order", convert.getValidRecords().get(0).getColumns().get("order_number"));
        Assert.assertEquals("test", convert.getValidRecords().get(0).getColumns().get("order_url"));
    }


    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfParserFails() {
        ProtoUpdateListener protoUpdateListener = new ProtoUpdateListener(config, bigQueryClient, converterWrapper);

        HashMap<String, DescriptorAndTypeName> descriptorsMap = new HashMap<String, DescriptorAndTypeName>() {{
            put(String.format("%s.%s", TestKeyBQ.class.getPackage(), TestKeyBQ.class.getName()), new DescriptorAndTypeName(TestKeyBQ.getDescriptor(), String.format(".%s.%s", TestKeyBQ.getDescriptor().getFile().getPackage(), TestKeyBQ.getDescriptor().getName())));
        }};
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");

        protoUpdateListener.onProtoUpdate("", descriptorsMap);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfConverterFails() throws IOException {
        ProtoUpdateListener protoUpdateListener = new ProtoUpdateListener(config, bigQueryClient, converterWrapper);
        ProtoField returnedProtoField = new ProtoField();
        returnedProtoField.addField(ProtoUtil.createProtoField("order_number", 1));
        returnedProtoField.addField(ProtoUtil.createProtoField("order_url", 2));

        HashMap<String, DescriptorAndTypeName> descriptorsMap = new HashMap<String, DescriptorAndTypeName>() {{
            put(String.format("%s.%s", TestKeyBQ.class.getPackage(), TestKeyBQ.class.getName()), new DescriptorAndTypeName(TestKeyBQ.getDescriptor(), String.format(".%s.%s", TestKeyBQ.getDescriptor().getFile().getPackage(), TestKeyBQ.getDescriptor().getName())));
        }};
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            addAll(MetadataUtil.getMetadataFields());
        }};
        doThrow(new BigQueryException(10, "bigquery mapping has failed")).when(bigQueryClient).upsertTable(bqSchemaFields);

        protoUpdateListener.onProtoUpdate("", descriptorsMap);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfDatasetLocationIsChanged() throws IOException {
        ProtoUpdateListener protoUpdateListener = new ProtoUpdateListener(config, bigQueryClient, converterWrapper);

        ProtoField returnedProtoField = new ProtoField();
        returnedProtoField.addField(ProtoUtil.createProtoField("order_number", 1));
        returnedProtoField.addField(ProtoUtil.createProtoField("order_url", 2));

        HashMap<String, DescriptorAndTypeName> descriptorsMap = new HashMap<String, DescriptorAndTypeName>() {{
            put(String.format("%s.%s", TestKeyBQ.class.getPackage(), TestKeyBQ.class.getName()), new DescriptorAndTypeName(TestKeyBQ.getDescriptor(), String.format(".%s.%s", TestKeyBQ.getDescriptor().getFile().getPackage(), TestKeyBQ.getDescriptor().getName())));
        }};
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            addAll(MetadataUtil.getMetadataFields());
        }};
        doThrow(new RuntimeException("cannot change dataset location")).when(bigQueryClient).upsertTable(bqSchemaFields);

        protoUpdateListener.onProtoUpdate("", descriptorsMap);
    }

    @Test
    public void shouldNotNamespaceMetadataFieldsWhenNamespaceIsNotProvided() throws IOException {
        ProtoUpdateListener protoUpdateListener = new ProtoUpdateListener(config, bigQueryClient, converterWrapper);

        ProtoField returnedProtoField = new ProtoField();
        returnedProtoField.addField(ProtoUtil.createProtoField("order_number", 1));
        returnedProtoField.addField(ProtoUtil.createProtoField("order_url", 2));

        HashMap<String, DescriptorAndTypeName> descriptorsMap = new HashMap<String, DescriptorAndTypeName>() {{
            put(String.format("%s", TestKeyBQ.class.getName()), new DescriptorAndTypeName(TestKeyBQ.getDescriptor(), String.format(".%s.%s", TestKeyBQ.getDescriptor().getFile().getPackage(), TestKeyBQ.getDescriptor().getName())));
        }};
        when(stencilClient.get(TestKeyBQ.class.getName())).thenReturn(descriptorsMap.get(TestKeyBQ.class.getName()).getDescriptor());
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            addAll(MetadataUtil.getMetadataFields()); // metadata fields are not namespaced
        }};
        doNothing().when(bigQueryClient).upsertTable(bqSchemaFields);
        ProtoParser protoParser = new ProtoParser(stencilClient, config.getInputSchemaProtoClass());
        protoUpdateListener.setStencilParser(protoParser);
        protoUpdateListener.onProtoUpdate("", descriptorsMap);
        TestKeyBQ testKeyBQ = TestKeyBQ.newBuilder().setOrderNumber("order").setOrderUrl("test").build();
        Instant now = Instant.now();
        Message testMessage = new Message("".getBytes(), testKeyBQ.toByteArray(), "topic", 1, 1);
        Records convert = protoUpdateListener.getMessageRecordConverterCache().getMessageRecordConverter().convert(Collections.singletonList(testMessage), now);
        Assert.assertEquals(1, convert.getValidRecords().size());
        Assert.assertEquals("order", convert.getValidRecords().get(0).getColumns().get("order_number"));
        Assert.assertEquals("test", convert.getValidRecords().get(0).getColumns().get("order_url"));
        verify(bigQueryClient, times(1)).upsertTable(bqSchemaFields); // assert that metadata fields were not namespaced
    }

    @Test
    public void shouldNamespaceMetadataFieldsWhenNamespaceIsProvided() throws IOException {
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "metadata_ns");
        config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        ProtoUpdateListener protoUpdateListener = new ProtoUpdateListener(config, bigQueryClient, converterWrapper);

        ProtoField returnedProtoField = new ProtoField();
        returnedProtoField.addField(ProtoUtil.createProtoField("order_number", 1));
        returnedProtoField.addField(ProtoUtil.createProtoField("order_url", 2));

        HashMap<String, DescriptorAndTypeName> descriptorsMap = new HashMap<String, DescriptorAndTypeName>() {{
            put(String.format("%s", TestKeyBQ.class.getName()), new DescriptorAndTypeName(TestKeyBQ.getDescriptor(), String.format(".%s.%s", TestKeyBQ.getDescriptor().getFile().getPackage(), TestKeyBQ.getDescriptor().getName())));
        }};
        when(stencilClient.get(TestKeyBQ.class.getName())).thenReturn(descriptorsMap.get(TestKeyBQ.class.getName()).getDescriptor());
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(MetadataUtil.getNamespacedMetadataField(config.getBqMetadataNamespace())); // metadata fields are namespaced
        }};
        doNothing().when(bigQueryClient).upsertTable(bqSchemaFields);

        ProtoParser protoParser = new ProtoParser(stencilClient, config.getInputSchemaProtoClass());
        protoUpdateListener.setStencilParser(protoParser);
        protoUpdateListener.onProtoUpdate("", descriptorsMap);
        TestKeyBQ testKeyBQ = TestKeyBQ.newBuilder().setOrderNumber("order").setOrderUrl("test").build();
        Instant now = Instant.now();
        Message testMessage = new Message("".getBytes(), testKeyBQ.toByteArray(), "topic", 1, 1);
        Records convert = protoUpdateListener.getMessageRecordConverterCache().getMessageRecordConverter().convert(Collections.singletonList(testMessage), now);
        Assert.assertEquals(1, convert.getValidRecords().size());
        Assert.assertEquals("order", convert.getValidRecords().get(0).getColumns().get("order_number"));
        Assert.assertEquals("test", convert.getValidRecords().get(0).getColumns().get("order_url"));

        verify(bigQueryClient, times(1)).upsertTable(bqSchemaFields);
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "");
    }

    @Test
    public void shouldThrowExceptionWhenMetadataNamespaceNameCollidesWithAnyFieldName() throws IOException {
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "order_number"); // set field name to an existing column name
        config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        ProtoUpdateListener protoUpdateListener = new ProtoUpdateListener(config, bigQueryClient, converterWrapper);

        ProtoField returnedProtoField = new ProtoField();
        returnedProtoField.addField(ProtoUtil.createProtoField("order_number", 1));
        returnedProtoField.addField(ProtoUtil.createProtoField("order_url", 2));

        HashMap<String, DescriptorAndTypeName> descriptorsMap = new HashMap<String, DescriptorAndTypeName>() {{
            put(String.format("%s", TestKeyBQ.class.getName()), new DescriptorAndTypeName(TestKeyBQ.getDescriptor(), String.format(".%s.%s", TestKeyBQ.getDescriptor().getFile().getPackage(), TestKeyBQ.getDescriptor().getName())));
        }};
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(MetadataUtil.getNamespacedMetadataField(config.getBqMetadataNamespace()));
        }};

        Exception exception = Assertions.assertThrows(RuntimeException.class, () -> {
            protoUpdateListener.onProtoUpdate("", descriptorsMap);
        });
        Assert.assertEquals("Metadata field(s) is already present in the schema. fields: [order_number]", exception.getMessage());
        verify(bigQueryClient, times(0)).upsertTable(bqSchemaFields);
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "");
    }

}
