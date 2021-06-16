package io.odpf.firehose.sink.bq;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.LegacySQLTypeName;
import io.odpf.firehose.config.BQSinkConfig;
import io.odpf.firehose.exception.BQPartitionKeyNotSpecified;
import lombok.AllArgsConstructor;

import java.util.Optional;

@AllArgsConstructor
public class BQTableDefinition {
    private BQSinkConfig bqSinkConfig;

    public StandardTableDefinition getTableDefinition(Schema schema) {
        StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .build();
        if (!bqSinkConfig.isBQTablePartitioningEnabled()) {
            return tableDefinition;
        }
        return getPartitionedTableDefinition(schema);
    }

    private StandardTableDefinition getPartitionedTableDefinition(Schema schema) {
        StandardTableDefinition.Builder tableDefinition = StandardTableDefinition.newBuilder();
        Optional<Field> partitionFieldOptional = schema.getFields().stream().filter(obj -> obj.getName().equals(bqSinkConfig.getBQTablePartitionKey())).findFirst();
        if (!partitionFieldOptional.isPresent()) {
            throw new BQPartitionKeyNotSpecified(String.format("Partition key %s is not present in the schema", bqSinkConfig.getBQTablePartitionKey()));
        }

        Field partitionField = partitionFieldOptional.get();
        if (isTimePartitionedField(partitionField)) {
            return createTimePartitionBuilder(tableDefinition)
                    .setSchema(schema)
                    .build();
        } else {
            throw new UnsupportedOperationException("Range Bigquery partitioning is not supported, supported paritition fields have to be of DATE or TIMESTAMP type");
        }
    }

    private StandardTableDefinition.Builder createTimePartitionBuilder(StandardTableDefinition.Builder tableBuilder) {
        TimePartitioning.Builder timePartitioningBuilder = TimePartitioning.newBuilder(TimePartitioning.Type.DAY);
        if (bqSinkConfig.getBQTablePartitionKey() == null) {
            throw new BQPartitionKeyNotSpecified(String.format("Partition key not specified for the table: %s", bqSinkConfig.getTable()));
        }
        timePartitioningBuilder.setField(bqSinkConfig.getBQTablePartitionKey())
                .setRequirePartitionFilter(true);

        Long neverExpireMillis = null;
        Long partitionExpiry = bqSinkConfig.getBQTablePartitionExpiryMillis() > 0 ? bqSinkConfig.getBQTablePartitionExpiryMillis() : neverExpireMillis;
        timePartitioningBuilder.setExpirationMs(partitionExpiry);

        return tableBuilder
                .setTimePartitioning(timePartitioningBuilder.build());
    }

    private boolean isTimePartitionedField(Field partitionField) {
        return partitionField.getType() == LegacySQLTypeName.TIMESTAMP || partitionField.getType() == LegacySQLTypeName.DATE;
    }
}

