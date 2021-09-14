package io.odpf.firehose.config.converter;

import io.odpf.firehose.config.enums.FilterDataSourceType;
import org.aeonbits.owner.Converter;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;

public class FilterDataSourceTypeConverter implements Converter<FilterDataSourceType> {
    @Override
    public FilterDataSourceType convert(Method method, String input) {
        if (StringUtils.isNotEmpty(input)) {
            return FilterDataSourceType.valueOf(input.toUpperCase());
        } else {
            return FilterDataSourceType.NONE;
        }
    }
}
