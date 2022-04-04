package io.odpf.firehose.config;

import io.odpf.firehose.config.enums.InputDataType;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AppConfigTest {

    @Test
    public void getInputDataTypeProtoBuf() {
        // when default
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, Collections.emptyMap());
        InputDataType defaultDataType = appConfig.getInputDataTye();
        assertEquals(InputDataType.PROTOBUF, defaultDataType);

        //when explicitly set as protobuf
        Map<Object, Object> configMap = new HashMap<>();
        configMap.put("INPUT_DATA_TYPE", "protobuf");
        AppConfig appConfigExplicit = ConfigFactory.create(AppConfig.class, configMap);
        InputDataType protobufDataType = appConfigExplicit.getInputDataTye();
        assertEquals(InputDataType.PROTOBUF, protobufDataType);
    }

    @Test
    public void getInputDataTypeJson() {
        Map<Object, Object> configMap = new HashMap<>();
        configMap.put("INPUT_DATA_TYPE", "json");
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, configMap);
        InputDataType defaultDataType = appConfig.getInputDataTye();
        assertEquals(InputDataType.JSON, defaultDataType);
    }
}
