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

    @Key("SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE")
    @DefaultValue("false")
    Boolean isTablePartitioningEnabled();

    @Key("SINK_BIGQUERY_TABLE_PARTITION_KEY")
    String getTablePartitionKey();

    @Key("SINK_BIGQUERY_ROW_INSERT_ID_ENABLE")
    @DefaultValue("true")
    Boolean isRowInsertIdEnabled();

    @Key("SINK_BIGQUERY_CLIENT_READ_TIMEOUT_MS")
    @DefaultValue("-1")
    int getBqClientReadTimeoutMS();

    @Key("SINK_BIGQUERY_CLIENT_CONNECT_TIMEOUT_MS")
    @DefaultValue("-1")
    int getBqClientConnectTimeoutMS();

    @Key("SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS")
    @DefaultValue("-1")
    Long getBigQueryTablePartitionExpiryMS();

    @Key("SINK_BIGQUERY_DATASET_LOCATION")
    @DefaultValue("asia-southeast1")
    String getBigQueryDatasetLocation();

    @DefaultValue("")
    @Key("SINK_BIGQUERY_METADATA_NAMESPACE")
    String getBqMetadataNamespace();
}
