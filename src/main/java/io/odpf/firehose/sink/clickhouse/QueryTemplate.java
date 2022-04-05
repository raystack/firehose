package io.odpf.firehose.sink.clickhouse;

import com.samskivert.mustache.Escapers;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import io.odpf.firehose.config.ClickhouseSinkConfig;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.proto.ProtoToFieldMapper;

import java.util.List;
import java.util.HashMap;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Query template for clickhouse.
 */
public class QueryTemplate {
    private static final String INSERT_QUERY_TEMPLATE_STRING = "INSERT INTO {{table}} ( {{insertColumns}} ) values ( {{insertValues}} ) ";
    private static final String DELIMITER_FOR_MULTIPLE_VALUES = " ),( "; // used for multiple values in a single query.
    private Template template;
    private ProtoToFieldMapper protoToFieldMapper;
    private List<String> insertColumns;
    private HashMap<String, Object> templateContexts;
    private String kafkaRecordParserMode;

    public QueryTemplate(ClickhouseSinkConfig clickhouseSinkConfig, ProtoToFieldMapper protoToFieldMapper) {
        this.protoToFieldMapper = protoToFieldMapper;
        this.insertColumns = new ArrayList<>();
        this.templateContexts = new HashMap<>();
        this.kafkaRecordParserMode = clickhouseSinkConfig.getKafkaRecordParserMode();
        template = Mustache.compiler().withEscaper(Escapers.simple()).compile(INSERT_QUERY_TEMPLATE_STRING);
    }

    public void initialize(ClickhouseSinkConfig clickhouseSinkConfig) {
        templateContexts.put("table", clickhouseSinkConfig.getClickhouseTableName());

        Properties messageProtoToDBColumnsMapping = clickhouseSinkConfig.getInputSchemaProtoToColumnMapping();
        insertColumns = getInsertColumns(messageProtoToDBColumnsMapping);
        templateContexts.put("insertColumns", String.join(",", insertColumns));
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

    public String toQueryStringForSingleMessage(Message message) {
        byte[] value;
        if ("message".equals(kafkaRecordParserMode)) {
            value = message.getLogMessage();
        } else {
            value = message.getLogKey();
        }
        Map<String, Object> columnToValue = protoToFieldMapper.getFields(value);
        String insertValues = stringifyColumnValues(columnToValue, insertColumns);
        templateContexts.put("insertValues", insertValues);
        return template.execute(templateContexts);
    }

    private String stringifyColumnValues(Map<String, Object> columnToValue, List<String> columns) {
        List<String> columnValues = columns.stream()
                .map(c -> columnToValue.get(c).toString().replace("'", "''"))
                .map(c -> "\'" + c + "\'")
                .collect(Collectors.toList());
        return String.join(", ", columnValues);
    }

    public String toQueryStringForMultipleMessages(List<Message> messages) {
        List<String> insertValues = new ArrayList<>();
        messages.stream().forEach((message) -> {
            byte[] value;
            if ("message".equals(kafkaRecordParserMode)) {
                value = message.getLogMessage();
            } else {
                value = message.getLogKey();
            }
            Map<String, Object> columnToValue = protoToFieldMapper.getFields(value);
            insertValues.add(stringifyColumnValues(columnToValue,insertColumns));
        });
        templateContexts.put("insertValues", String.join(DELIMITER_FOR_MULTIPLE_VALUES,insertValues));
        return template.execute(templateContexts);
    }
}
