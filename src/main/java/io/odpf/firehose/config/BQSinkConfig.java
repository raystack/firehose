package io.odpf.firehose.config;

import io.odpf.firehose.config.converter.LabelMapConverter;

import java.util.Map;

public interface BQSinkConfig extends AppConfig {

    @Key("BQ_PROJECT_NAME")
    String getGCPProject();

    @Key("BQ_TABLE_NAME")
    String getTable();

    @Key("BQ_DATASET_LABELS")
    @Separator(LabelMapConverter.ELEMENT_SEPARATOR)
    @ConverterClass(LabelMapConverter.class)
    Map<String, String> getDatasetLabels();

    @Key("BQ_TABLE_LABELS")
    @Separator(LabelMapConverter.ELEMENT_SEPARATOR)
    @ConverterClass(LabelMapConverter.class)
    Map<String, String> getTableLabels();

    @Key("BQ_DATASET_NAME")
    String getDataset();

    @Key("GOOGLE_CREDENTIALS")
    String getGoogleCredentials();

    @Key("ENABLE_BQ_TABLE_PARTITIONING")
    @DefaultValue("false")
    Boolean isBQTablePartitioningEnabled();

    @Key("BQ_TABLE_PARTITION_KEY")
    String getBQTablePartitionKey();

    @DefaultValue("true")
    @Key("ENABLE_BQ_ROW_INSERTID")
    Boolean isBQRowInsertIdEnabled();

    @DefaultValue("-1")
    @Key("BQ_CLIENT_READ_TIMEOUT")
    String getBqClientReadTimeout();

    @DefaultValue("-1")
    @Key("BQ_CLIENT_CONNECT_TIMEOUT")
    String getBqClientConnectTimeout();

    @DefaultValue("-1")
    @Key("BQ_TABLE_PARTITION_EXPIRY_MILLIS")
    Long getBQTablePartitionExpiryMillis();

    @DefaultValue("US")
    @Key("BQ_DATASET_LOCATION")
    String getBQDatasetLocation();

    @DefaultValue("false")
    @Key("ENABLE_AUTO_SCHEMA_UPDATE")
    Boolean isAutoSchemaUpdateEnabled();
}
