package io.odpf.firehose.sink.bq;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import io.odpf.firehose.config.BQSinkConfig;
import io.odpf.firehose.exception.BQDatasetLocationChangedException;
import io.odpf.firehose.exception.BQPartitionKeyNotSpecified;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;

@Slf4j
public class BQClient {
    private BigQuery bigquery;
    private TableId tableID;
    private Instrumentation instrumentation;
    private BQSinkConfig bqSinkConfig;
    private BQTableDefinition bqTableDefinition;

    public BQClient(BigQuery bigquery, Instrumentation instrumentation, BQSinkConfig bqSinkConfig) {
        this.bigquery = bigquery;
        this.instrumentation = instrumentation;
        this.bqSinkConfig = bqSinkConfig;
        this.tableID = TableId.of(bqSinkConfig.getDataset(), bqSinkConfig.getTable());
        this.bqTableDefinition = new BQTableDefinition(bqSinkConfig);
    }

    public void upsertTable(List<Field> bqSchemaFields) throws BigQueryException {
        Schema schema = Schema.of(bqSchemaFields);
        TableDefinition tableDefinition = getTableDefinition(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableID, tableDefinition)
                .setLabels(bqSinkConfig.getTableLabels())
                .build();
        upsertDatasetAndTable(tableInfo);
    }

    private void upsertDatasetAndTable(TableInfo tableInfo) {
        Dataset dataSet = bigquery.getDataset(tableID.getDataset());
        if (dataSet == null || !bigquery.getDataset(tableID.getDataset()).exists()) {
            bigquery.create(
                    Dataset.newBuilder(tableID.getDataset())
                            .setLocation(bqSinkConfig.getBQDatasetLocation())
                            .setLabels(bqSinkConfig.getDatasetLabels())
                            .build()
            );
            log.info("Successfully CREATED bigquery DATASET: {}", tableID.getDataset());
        } else if (shouldUpdateDataset(dataSet)) {
            bigquery.update(
                    Dataset.newBuilder(tableID.getDataset())
                            .setLabels(bqSinkConfig.getDatasetLabels())
                            .build()
            );
            log.info("Successfully UPDATED bigquery DATASET: {} with labels", tableID.getDataset());
        }

        Table table = bigquery.getTable(tableID);
        if (table == null || !table.exists()) {
            bigquery.create(tableInfo);
            log.info("Successfully CREATED bigquery TABLE: {}", tableID.getTable());
        } else {
            Schema existingSchema = table.getDefinition().getSchema();
            Schema updatedSchema = tableInfo.getDefinition().getSchema();

            if (shouldUpdateTable(tableInfo, table, existingSchema, updatedSchema)) {
                Instant start = Instant.now();
                bigquery.update(tableInfo);
                log.info("Successfully UPDATED bigquery TABLE: {}", tableID.getTable());
            } else {
                log.info("Skipping bigquery table update, since proto schema hasn't changed");
            }
        }
    }

    private boolean shouldUpdateTable(TableInfo tableInfo, Table table, Schema existingSchema, Schema updatedSchema) {
        return !table.getLabels().equals(tableInfo.getLabels())
                || !existingSchema.equals(updatedSchema)
                || shouldChangePartitionExpiryForStandardTable(table);
    }

    private boolean shouldUpdateDataset(Dataset dataSet) {
        if (!dataSet.getLocation().equals(bqSinkConfig.getBQDatasetLocation())) {
            throw new BQDatasetLocationChangedException("Dataset location cannot be changed from "
                    + dataSet.getLocation() + " to " + bqSinkConfig.getBQDatasetLocation());
        }

        return !dataSet.getLabels().equals(bqSinkConfig.getDatasetLabels());
    }

    private boolean shouldChangePartitionExpiryForStandardTable(Table table) {
        if (!table.getDefinition().getType().equals(TableDefinition.Type.TABLE)) {
            return false;
        }
        TimePartitioning timePartitioning = ((StandardTableDefinition) (table.getDefinition())).getTimePartitioning();
        if (timePartitioning == null) {
            return false;
        }
        Long neverExpireMs = 0L;
        Long currentExpirationMs = timePartitioning.getExpirationMs() == null ? neverExpireMs : timePartitioning.getExpirationMs();
        Long newExpirationMs = bqSinkConfig.getBQTablePartitionExpiryMillis() > 0 ? bqSinkConfig.getBQTablePartitionExpiryMillis() : neverExpireMs;
        return !currentExpirationMs.equals(newExpirationMs);
    }

    private TableDefinition getTableDefinition(Schema schema) throws BQPartitionKeyNotSpecified {
        return bqTableDefinition.getTableDefinition(schema);
    }
}

