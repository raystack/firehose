package io.odpf.firehose.config.converter;

import io.odpf.firehose.config.enums.InputSchemaDataType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class InputSchemaDataTypeConverter implements Converter {
    @Override
    public InputSchemaDataType convert(Method method, String input) {
        return InputSchemaDataType.valueOf(input.toUpperCase());
    }
}
