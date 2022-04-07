package io.odpf.firehose.config;

import io.odpf.firehose.config.enums.InputSchemaDataType;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AppConfigTest {

    @Test
    public void getInputSchemaDataTypeProtoBuf() {
        // when default
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, Collections.emptyMap());
        InputSchemaDataType defaultDataType = appConfig.getInputSchemaDataTye();
        assertEquals(InputSchemaDataType.PROTOBUF, defaultDataType);

        //when explicitly set as protobuf
        Map<Object, Object> configMap = new HashMap<>();
        configMap.put("INPUT_SCHEMA_DATA_TYPE", "protobuf");
        AppConfig appConfigExplicit = ConfigFactory.create(AppConfig.class, configMap);
        InputSchemaDataType protobufDataType = appConfigExplicit.getInputSchemaDataTye();
        assertEquals(InputSchemaDataType.PROTOBUF, protobufDataType);
    }

    @Test
    public void getInpuSchematDataTypeJson() {
        Map<Object, Object> configMap = new HashMap<>();
        configMap.put("INPUT_SCHEMA_DATA_TYPE", "json");
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, configMap);
        InputSchemaDataType defaultDataType = appConfig.getInputSchemaDataTye();
        assertEquals(InputSchemaDataType.JSON, defaultDataType);
    }
}
