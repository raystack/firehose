package io.odpf.firehose.sink.bigquery.handler;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import io.odpf.firehose.config.BigQuerySinkConfig;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;

@Slf4j
public class BigQueryClient {
    private final BigQuery bigquery;
    private final TableId tableID;
    private final BigQuerySinkConfig bqConfig;
    private final BQTableDefinition bqTableDefinition;

    public BigQueryClient(BigQuery bigquery, BigQuerySinkConfig bqConfig) {
        this.bigquery = bigquery;
        this.bqConfig = bqConfig;
        this.tableID = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        this.bqTableDefinition = new BQTableDefinition(bqConfig);
    }

    public void upsertTable(List<Field> bqSchemaFields) throws BigQueryException {
        Schema schema = Schema.of(bqSchemaFields);
        TableDefinition tableDefinition = getTableDefinition(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableID, tableDefinition)
                .setLabels(bqConfig.getTableLabels())
                .build();
        upsertDatasetAndTable(tableInfo);
    }

    private void upsertDatasetAndTable(TableInfo tableInfo) {
        Dataset dataSet = bigquery.getDataset(tableID.getDataset());
        if (dataSet == null || !bigquery.getDataset(tableID.getDataset()).exists()) {
            bigquery.create(
                    Dataset.newBuilder(tableID.getDataset())
                            .setLocation(bqConfig.getBigQueryDatasetLocation())
                            .setLabels(bqConfig.getDatasetLabels())
                            .build()
            );
            log.info("Successfully CREATED bigquery DATASET: {}", tableID.getDataset());
        } else if (shouldUpdateDataset(dataSet)) {
            bigquery.update(
                    Dataset.newBuilder(tableID.getDataset())
                            .setLabels(bqConfig.getDatasetLabels())
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
//                instrumentation.captureDurationSince("bq.upsert.table.time," + statsClient.getBqTags(), start);
                //               instrumentation.incrementCounter("bq.upsert.table.count," + statsClient.getBqTags());
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
        if (!dataSet.getLocation().equals(bqConfig.getBigQueryDatasetLocation())) {
            throw new RuntimeException("Dataset location cannot be changed from "
                                       + dataSet.getLocation() + " to " + bqConfig.getBigQueryDatasetLocation());
        }

        return !dataSet.getLabels().equals(bqConfig.getDatasetLabels());
    }

    private boolean shouldChangePartitionExpiryForStandardTable(Table table) {
        if (!table.getDefinition().getType().equals(TableDefinition.Type.TABLE)) {
            return false;
        }
        TimePartitioning timePartitioning = ((StandardTableDefinition) (table.getDefinition())).getTimePartitioning();
        if (timePartitioning == null) {
            // If the table is not partitioned already, no need to update the table
            return false;
        }
        long neverExpireMs = 0L;
        Long currentExpirationMs = timePartitioning.getExpirationMs() == null ? neverExpireMs : timePartitioning.getExpirationMs();
        Long newExpirationMs = bqConfig.getBigQueryTablePartitionExpiryMillis() > 0 ? bqConfig.getBigQueryTablePartitionExpiryMillis() : neverExpireMs;
        return !currentExpirationMs.equals(newExpirationMs);
    }

    private TableDefinition getTableDefinition(Schema schema) throws RuntimeException {
        return bqTableDefinition.getTableDefinition(schema);
    }
}
