package org.raystack.firehose.config.converter;

import org.raystack.firehose.config.enums.FilterDataSourceType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class FilterDataSourceTypeConverter implements Converter<FilterDataSourceType> {
    @Override
    public FilterDataSourceType convert(Method method, String input) {
        try {
            return FilterDataSourceType.valueOf(input.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("FILTER_DATA_SOURCE must be or KEY or MESSAGE", e);
        }
    }
}
