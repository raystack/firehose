package com.gojek.esb.sink.db;

import com.gojek.esb.config.DBSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.proto.ProtoToFieldMapper;
import com.samskivert.mustache.Escapers;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryTemplate {
    private static final String INSERT_QUERY = "INSERT INTO {{table}} ( {{insertColumns}} ) values ( {{insertValues}} ) ";
    private static final String UPDATE_CLAUSE = "ON CONFLICT ( {{unique}} ) DO UPDATE SET ( {{updateColumns}} ) = ({{updateValues}})";
    private static final String EMPTY_UPDATE_CLAUSE = "ON CONFLICT ( {{unique}} ) DO NOTHING";
    private final Boolean auditEnabled;
    private Template template;
    private ProtoToFieldMapper protoToFieldMapper;
    private List<String> insertColumns;
    private List<String> updateColumns;
    private Set<String> uniqueColumns;
    private HashMap<String, Object> scopes;
    private String kafkaRecordParserMode;

    public QueryTemplate(DBSinkConfig dbSinkConfig, ProtoToFieldMapper protoToFieldMapper) {
        this.protoToFieldMapper = protoToFieldMapper;
        this.insertColumns = new ArrayList<>();
        this.updateColumns = new ArrayList<>();
        this.uniqueColumns = new HashSet<>();
        this.scopes = new HashMap<>();
        this.auditEnabled = dbSinkConfig.getAuditEnabled();
        this.kafkaRecordParserMode = dbSinkConfig.getKafkaRecordParserMode();

        initialize(dbSinkConfig);
        buildQuery();
    }

    private void buildQuery() {
        String query = isAnUpsertOperation() ? INSERT_QUERY + onConflictResolutionQuery() : INSERT_QUERY;
        template = Mustache.compiler().withEscaper(Escapers.simple()).compile(query);
    }

    private boolean isAnUpsertOperation() {
        return uniqueColumns.size() != 0;
    }

    private void initialize(DBSinkConfig dbSinkConfig) {
        String uniqueKeys = dbSinkConfig.getUniqueKeys();
        scopes.put("unique", uniqueKeys);
        scopes.put("table", dbSinkConfig.getTableName());

        uniqueColumns = Arrays.stream(uniqueKeys.split(","))
                .map(String::trim)
                .filter(e -> !e.isEmpty())
                .collect(Collectors.toSet());

        Properties messageProtoToDBColumnsMapping = dbSinkConfig.getProtoToFieldMapping();
        insertColumns = getInsertColumns(messageProtoToDBColumnsMapping);

        if (this.auditEnabled) {
            insertColumns.addAll(Arrays.asList("kafka_audit_topic", "kafka_audit_partition", "kafka_audit_offset"));
        }

        updateColumns = selectNonUniqueKeyColumns(uniqueKeys, insertColumns);

        scopes.put("insertColumns", String.join(",", insertColumns));
        scopes.put("updateColumns", String.join(",", updateColumns));
    }

    private List<String> getInsertColumns(Properties messageProtoToDBColumnsMapping) {
        List<String> columns = new ArrayList<>();
        Enumeration<?> propertyNames = messageProtoToDBColumnsMapping.propertyNames();
        while (propertyNames.hasMoreElements()) {
            Object tableColumn = messageProtoToDBColumnsMapping.get(propertyNames.nextElement());
            if (tableColumn instanceof String) {
                columns.add((String) tableColumn);
            } else if (tableColumn instanceof Properties) {
                columns.addAll(getInsertColumns((Properties) tableColumn));
            }
        }
        return columns;
    }

    private List<String> selectNonUniqueKeyColumns(String uniqueKeys, List<String> columnsToFilter) {
        return columnsToFilter.stream().filter(colunmName -> !uniqueKeys.contains(colunmName)).collect(Collectors.toList());
    }

    public String toQueryString(EsbMessage message) {

        byte[] value;

        if ("message".equals(kafkaRecordParserMode)) {
            value = message.getLogMessage();
        } else {
            value = message.getLogKey();
        }

        Map<String, Object> columnToValue = protoToFieldMapper.getFields(value);
        if (auditEnabled) {
            columnToValue.put("kafka_audit_topic", message.getTopic());
            columnToValue.put("kafka_audit_partition", message.getPartition());
            columnToValue.put("kafka_audit_offset", message.getOffset());
        }

        String insertValues = stringifyColumnValues(columnToValue, insertColumns);
        String updateValues = stringifyColumnValues(columnToValue, updateColumns);

        scopes.put("updateValues", updateValues);
        scopes.put("insertValues", insertValues);

        return template.execute(scopes);
    }

    private String stringifyColumnValues(Map<String, Object> columnToValue, List<String> columns) {
        List<String> columnValues = columns.stream()
                .map(c -> columnToValue.get(c).toString().replace("'", "''"))
                .map(c -> "\'" + c + "\'")
                .collect(Collectors.toList());
        return String.join(", ", columnValues);
    }

    private String onConflictResolutionQuery() {
        return updateColumns.size() == 0 ? EMPTY_UPDATE_CLAUSE : UPDATE_CLAUSE;
    }
}
