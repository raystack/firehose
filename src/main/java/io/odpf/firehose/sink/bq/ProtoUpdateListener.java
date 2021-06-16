package io.odpf.firehose.sink.bq;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.models.DescriptorAndTypeName;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.de.stencil.parser.ProtoParserWithRefresh;
import com.gojek.de.stencil.utils.StencilUtils;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.thoughtworks.xstream.converters.ErrorWriter;
import io.odpf.firehose.config.BQSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class ProtoUpdateListener extends com.gojek.de.stencil.cache.ProtoUpdateListener {
    private final String proto;
    private final BQSinkConfig bqSinkConfig;
    private ConsumerRecordConverter recordConverter;
    private StencilClient stencilClient;
    private Converter protoMappingConverter;
    private Parser protoMappingParser;
    private BQClient bqClient;
    private ProtoFieldFactory protoFieldFactory;
    private Stats statsClient = Stats.client();
    private ErrorWriter errorWriter;

    public ProtoUpdateListener(BQSinkConfig bqSinkConfig, Converter protoMappingConverter, Parser protoMappingParser, BigQuery bqInstance, ErrorWriter errorWriter) throws IOException {
        super(bqSinkConfig.getInputSchemaProtoClass());
        this.proto = bqSinkConfig.getInputSchemaProtoClass();
        this.bqSinkConfig = bqSinkConfig;
        this.protoMappingConverter = protoMappingConverter;
        this.protoMappingParser = protoMappingParser;
        this.protoFieldFactory = new ProtoFieldFactory();
        this.bqClient = new BQClient(bqInstance, new Instrumentation(), bqSinkConfig);
        this.errorWriter = errorWriter;
        this.createStencilClient();
        this.setProtoParser(getProtoMapping());
    }

    private void createStencilClient() {
        try {
            if (bqSinkConfig.isAutoSchemaUpdateEnabled()) {
                stencilClient = StencilClientFactory.getClient(bqSinkConfig.getSchemaRegistryStencilUrls(), System.getenv(), Stats.client().getStatsDClient(), this);

                log.info("updating bq table at startup for proto schema {}", getProto());
                onProtoUpdate(bqSinkConfig.getSchemaRegistryStencilUrls(), stencilClient.getAllDescriptorAndTypeName());
            } else {
                stencilClient = StencilClientFactory.getClient(bqSinkConfig.getSchemaRegistryStencilUrls(), System.getenv(), Stats.client().getStatsDClient());
            }
        } catch (RuntimeException e) {
            emitStencilExceptionMetrics(e);
            throw e;
        }
    }

    @Override
    public void onProtoUpdate(String url, Map<String, DescriptorAndTypeName> newDescriptors) {
        log.info("stencil cache was refreshed, validating if bigquery schema changed");
        try {
            ProtoField protoField = protoFieldFactory.getProtoField();
            protoField = protoMappingParser.parseFields(protoField, proto, StencilUtils.getAllProtobufDescriptors(newDescriptors), StencilUtils.getTypeNameToPackageNameMap(newDescriptors));
            updateProtoParser(protoField);
        } catch (BigQueryException | ProtoNotFoundException | BQSchemaMappingException | BQPartitionKeyNotSpecified
                | BQDatasetLocationChangedException | IOException e) {
            String errMsg = "Error while updating bigquery table on callback:" + e.getMessage();
            log.error(errMsg);
            statsClient.increment("bq.table.upsert.failures");
            throw new BQTableUpdateFailure(errMsg, e);
        }
    }

    // First get latest protomapping, update bq schema, and if all goes fine
    // then only update beast's proto mapping config
    private void updateProtoParser(final ProtoField protoField) throws IOException {
        String protoMappingString = protoMappingConverter.generateColumnMappings(protoField.getFields());
        List<Field> bqSchemaFields = protoMappingConverter.generateBigquerySchema(protoField);
        addMetadataFields(bqSchemaFields);
        bqClient.upsertTable(bqSchemaFields);
        bqSinkConfig.setProperty("PROTO_COLUMN_MAPPING", protoMappingString);
        setProtoParser(bqSinkConfig.getProtoColumnMapping());
    }

    private void addMetadataFields(List<Field> bqSchemaFields) {
        List<Field> bqMetadataFields = new ArrayList<>();
        String namespaceName = appConfig.getBqMetadataNamespace();
        if (namespaceName.isEmpty()) {
            bqMetadataFields.addAll(BQField.getMetadataFields());
        } else {
            bqMetadataFields.add(BQField.getNamespacedMetadataField(namespaceName));
        }

        List<String> duplicateFields = getDuplicateFields(bqSchemaFields, bqMetadataFields).stream().map(Field::getName).collect(Collectors.toList());
        if (duplicateFields.size() > 0) {
            throw new BQSchemaMappingException(String.format("Metadata field(s) is already present in the schema. "
                    + "fields: %s", duplicateFields));
        }
        bqSchemaFields.addAll(bqMetadataFields);
    }

    private ColumnMapping getProtoMapping() throws IOException {
        ProtoField protoField = new ProtoField();
        protoField = protoMappingParser.parseFields(protoField, proto, stencilClient.getAll(), stencilClient.getTypeNameToPackageNameMap());
        String protoMapping = protoMappingConverter.generateColumnMappings(protoField.getFields());
        bqSinkConfig.setProperty("PROTO_COLUMN_MAPPING", protoMapping);
        return bqSinkConfig.getProtoColumnMapping();
    }

    public ConsumerRecordConverter getProtoParser() {
        return recordConverter;
    }

    private void setProtoParser(ColumnMapping columnMapping) {
        if (bqSinkConfig.getAutoRefreshCache()) {
            // periodic refresh
            ProtoParser protoParser = new ProtoParser(stencilClient, proto);
            recordConverter = new ConsumerRecordConverter(new RowMapper(columnMapping, bqSinkConfig.getFailOnUnknownFields()),
                    protoParser, new Clock(), appConfig, errorWriter);
        } else {
            // on-demand refresh
            ProtoParserWithRefresh protoParser = new ProtoParserWithRefresh(stencilClient, proto);
            recordConverter = new ConsumerRecordConverter(new RowMapper(columnMapping, bqSinkConfig.getFailOnUnknownFields()),
                    protoParser, new Clock(), appConfig, errorWriter);
        }
    }

    public void close() throws IOException {
        stencilClient.close();
    }

    private void emitStencilExceptionMetrics(RuntimeException e) {

    }

    private List<Field> getDuplicateFields(List<Field> fields1, List<Field> fields2) {
        return fields1.stream().filter(field -> containsField(fields2, field.getName())).collect(Collectors.toList());
    }

    private boolean containsField(List<Field> fields, String fieldName) {
        return fields.stream().anyMatch(field -> field.getName().equals(fieldName));
    }
}
