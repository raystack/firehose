package io.odpf.firehose.config.converter;

import io.odpf.firehose.config.enums.InputDataType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class InputDataTypeConverter implements Converter {
    @Override
    public InputDataType convert(Method method, String input) {
        return InputDataType.valueOf(input.toUpperCase());
    }
}
