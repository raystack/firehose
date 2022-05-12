package io.odpf.firehose.sink.bigquery.proto;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.Descriptors.Descriptor;

import io.odpf.firehose.config.BigQuerySinkConfig;
import io.odpf.firehose.sink.bigquery.converter.MessageRecordConverter;
import io.odpf.firehose.sink.bigquery.converter.MessageRecordConverterCache;
import io.odpf.firehose.sink.bigquery.converter.RowMapper;
import io.odpf.firehose.sink.bigquery.exception.BQSchemaMappingException;
import io.odpf.firehose.sink.bigquery.exception.BQTableUpdateFailure;
import io.odpf.firehose.sink.bigquery.handler.BigQueryClient;
import io.odpf.firehose.sink.bigquery.models.MetadataUtil;
import io.odpf.firehose.sink.bigquery.models.ProtoField;
import io.odpf.stencil.Parser;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class ProtoUpdateListener implements io.odpf.stencil.SchemaUpdateListener {
    private final BigQuerySinkConfig config;
    private final ProtoMapper protoMapper = new ProtoMapper();
    private final ProtoFieldParser protoMappingParser = new ProtoFieldParser();
    private final BigQueryClient bqClient;
    @Getter
    private final MessageRecordConverterCache messageRecordConverterCache;
    @Setter
    private Parser stencilParser;
    private static final Gson GSON = new Gson();

    public ProtoUpdateListener(BigQuerySinkConfig config, BigQueryClient bqClient, MessageRecordConverterCache messageRecordConverterCache) {
        this.config = config;
        this.bqClient = bqClient;
        this.messageRecordConverterCache = messageRecordConverterCache;
    }

    @Override
    public void onSchemaUpdate(Map<String, Descriptor> newDescriptors) {
        log.info("stencil cache was refreshed, validating if bigquery schema changed");
        try {
            ProtoField protoField = new ProtoField();
            protoField = protoMappingParser.parseFields(protoField, config.getInputSchemaProtoClass(), newDescriptors,
                    getTypeNameToPackageNameMap(newDescriptors));
            updateProtoParser(protoField);
        } catch (BigQueryException | IOException e) {
            String errMsg = "Error while updating bigquery table on callback:" + e.getMessage();
            log.error(errMsg);
            throw new BQTableUpdateFailure(errMsg, e);
        }
    }

    private Map<String, String> getTypeNameToPackageNameMap(Map<String, Descriptor> descriptors) {
        return descriptors.entrySet().stream()
                .collect(Collectors.toMap(
                        (mapEntry) -> String.format(".%s", mapEntry.getValue().getFullName()),
                        (mapEntry) -> mapEntry.getKey()));
    }

    // First get latest protomapping, update bq schema, and if all goes fine
    // then only update beast's proto mapping config
    private void updateProtoParser(final ProtoField protoField) throws IOException {
        String protoMappingString = protoMapper.generateColumnMappings(protoField.getFields());
        List<Field> bqSchemaFields = protoMapper.generateBigquerySchema(protoField);
        addMetadataFields(bqSchemaFields);
        bqClient.upsertTable(bqSchemaFields);
        setProtoParser(protoMappingString);
    }

    private Properties mapToProperties(Map<String, Object> inputMap) {
        Properties properties = new Properties();
        for (Map.Entry<String, Object> kv : inputMap.entrySet()) {
            if (kv.getValue() instanceof String) {
                properties.put(kv.getKey(), kv.getValue());
            } else if (kv.getValue() instanceof Map) {
                properties.put(kv.getKey(), mapToProperties((Map) kv.getValue()));
            }
        }
        return properties;
    }

    private void addMetadataFields(List<Field> bqSchemaFields) {
        List<Field> bqMetadataFields = new ArrayList<>();
        String namespaceName = config.getBqMetadataNamespace();
        if (namespaceName.isEmpty()) {
            bqMetadataFields.addAll(MetadataUtil.getMetadataFields());
        } else {
            bqMetadataFields.add(MetadataUtil.getNamespacedMetadataField(namespaceName));
        }

        List<String> duplicateFields = getDuplicateFields(bqSchemaFields, bqMetadataFields).stream().map(Field::getName).collect(Collectors.toList());
        if (duplicateFields.size() > 0) {
            throw new BQSchemaMappingException(String.format("Metadata field(s) is already present in the schema. "
                    + "fields: %s", duplicateFields));
        }
        bqSchemaFields.addAll(bqMetadataFields);
    }

    private void setProtoParser(String protoMapping) {
        Type type = new TypeToken<Map<String, Object>>() {
        }.getType();
        Map<String, Object> m = GSON.fromJson(protoMapping, type);
        Properties columnMapping = mapToProperties(m);
        messageRecordConverterCache.setMessageRecordConverter(
                new MessageRecordConverter(new RowMapper(columnMapping),
                        stencilParser, config));
    }

    public void close() throws IOException {
    }

    private List<Field> getDuplicateFields(List<Field> fields1, List<Field> fields2) {
        return fields1.stream().filter(field -> containsField(fields2, field.getName())).collect(Collectors.toList());
    }

    private boolean containsField(List<Field> fields, String fieldName) {
        return fields.stream().anyMatch(field -> field.getName().equals(fieldName));
    }
}
