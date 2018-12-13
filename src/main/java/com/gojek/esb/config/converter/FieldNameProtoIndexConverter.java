package com.gojek.esb.config.converter;

import com.google.gson.Gson;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;

public class FieldNameProtoIndexConverter implements Converter {
    @Override
    public Object convert(Method method, String input) {
        Map<String, String> m = new Gson().fromJson(input, Map.class);

        Properties messageColumnMapping = new Properties();
        for (String key : m.keySet()) {
            messageColumnMapping.put(key, m.get(key));
        }
        return messageColumnMapping;
    }
}
