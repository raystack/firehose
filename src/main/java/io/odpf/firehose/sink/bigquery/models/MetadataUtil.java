package io.odpf.firehose.sink.bigquery.models;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;

import java.util.ArrayList;
import java.util.List;

public class MetadataUtil {
    public static final List<Field> getMetadataFields() {
        return new ArrayList<Field>() {{
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};
    }

    public static Field getNamespacedMetadataField(String namespace) {
        return Field
                .newBuilder(namespace, LegacySQLTypeName.RECORD, FieldList.of(getMetadataFields()))
                .setMode(Field.Mode.NULLABLE)
                .build();
    }
}
