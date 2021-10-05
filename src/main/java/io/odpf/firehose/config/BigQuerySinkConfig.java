package io.odpf.firehose.config;

import io.odpf.firehose.config.converter.LabelMapConverter;

import java.util.Map;

public interface BigQuerySinkConfig extends AppConfig {

    @Key("SINK_BIGQUERY_GOOGLE_CLOUD_PROJECT_ID")
    String getGCloudProjectID();

    @Key("SINK_BIGQUERY_TABLE_NAME")
    String getTableName();

    @Key("SINK_BIGQUERY_DATASET_LABELS")
    @Separator(LabelMapConverter.ELEMENT_SEPARATOR)
    @ConverterClass(LabelMapConverter.class)
    Map<String, String> getDatasetLabels();

    @Key("SINK_BIGQUERY_TABLE_LABELS")
    @Separator(LabelMapConverter.ELEMENT_SEPARATOR)
    @ConverterClass(LabelMapConverter.class)
    Map<String, String> getTableLabels();

    @Key("SINK_BIGQUERY_DATASET_NAME")
    String getDatasetName();

    @Key("SINK_BIGQUERY_CREDENTIAL_PATH")
    String getBigQueryCredentialPath();

    @Key("SINK_BIGQUERY_ENABLE_TABLE_PARTITIONING_ENABLE")
    @DefaultValue("false")
    Boolean isTablePartitioningEnabled();

    @Key("SINK_BIGQUERY_TABLE_PARTITION_KEY")
    String getTablePartitionKey();

    @DefaultValue("true")
    @Key("SINK_BIGQUERY_ENABLE_ROW_INSERT_ID_ENABLE")
    Boolean isRowInsertIdEnabled();

    @DefaultValue("-1")
    @Key("SINK_BIGQUERY_CLIENT_READ_TIMEOUT_MS")
    int getBqClientReadTimeoutMS();

    @DefaultValue("-1")
    @Key("SINK_BIGQUERY_CLIENT_CONNECT_TIMEOUT_MS")
    int getBqClientConnectTimeoutMS();

    @DefaultValue("-1")
    @Key("SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS")
    Long getBigQueryTablePartitionExpiryMS();

    @DefaultValue("asia-southeast1")
    @Key("SINK_BIGQUERY_DATASET_LOCATION")
    String getBigQueryDatasetLocation();

    @Key("SINK_BIGQUERY_METADATA_NAMESPACE")
    @DefaultValue("")
    String getBqMetadataNamespace();
}
