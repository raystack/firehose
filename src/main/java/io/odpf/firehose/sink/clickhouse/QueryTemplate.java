package io.odpf.firehose.sink.clickhouse;

import com.samskivert.mustache.Escapers;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import io.odpf.firehose.config.ClickhouseSinkConfig;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.proto.ProtoToFieldMapper;

import java.util.*;
import java.util.stream.Collectors;
/**
 * Query template for clickhouse.
 */
public class QueryTemplate {
    private static final String INSERT_QUERY = "INSERT INTO {{table}} ( {{insertColumns}} ) values ( {{insertValues}} ) ";
    private static final String VALUES_SEPERATOR = " ),( "; // used for multiple values in a single query.
    private Template template;
    private ProtoToFieldMapper protoToFieldMapper;
    private List<String> insertColumns;
    private HashMap<String, Object> scopes;
    private String kafkaRecordParserMode;

    public QueryTemplate(ClickhouseSinkConfig clickhouseSinkConfig, ProtoToFieldMapper protoToFieldMapper) {
        this.protoToFieldMapper = protoToFieldMapper;
        this.insertColumns = new ArrayList<>();
        this.scopes = new HashMap<>();
        this.kafkaRecordParserMode = clickhouseSinkConfig.getKafkaRecordParserMode();
        initialize(clickhouseSinkConfig);
        buildQuery();
    }

    private void buildQuery() {
        String query = INSERT_QUERY;
        template = Mustache.compiler().withEscaper(Escapers.simple()).compile(query);
    }

    private void initialize(ClickhouseSinkConfig clickhouseSinkConfig) {
        scopes.put("table",clickhouseSinkConfig.getClickhouseTableName());

        Properties messageProtoToDBColumnsMapping = clickhouseSinkConfig.getInputSchemaProtoToColumnMapping();
        insertColumns = getInsertColumns(messageProtoToDBColumnsMapping);
        scopes.put("insertColumns", String.join(",", insertColumns));
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

    public String toQueryStringForMultipleMessages(List<Message> messages) {
        byte[] value;
        String insertValues = "";

        long count=0; //counter to maintain number of rows in the query.
        for(Message message: messages) {
            if ("message".equals(kafkaRecordParserMode)) {
                value = message.getLogMessage();
            } else {
                value = message.getLogKey();
            }
            count++;
            Map<String, Object> columnToValue = protoToFieldMapper.getFields(value);
            insertValues = insertValues + stringifyColumnValues(columnToValue, insertColumns) ;

            /*
            Value seperator is not needed if it's the last row to be added.
             */
            if(count!=messages.size())
                insertValues = insertValues + VALUES_SEPERATOR;
        }
        scopes.put("insertValues", insertValues);
        return template.execute(scopes);
    }
}
