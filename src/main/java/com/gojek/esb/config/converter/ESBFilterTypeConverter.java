package com.gojek.esb.config.converter;

import com.gojek.esb.config.enums.EsbFilterType;
import org.aeonbits.owner.Converter;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;

public class ESBFilterTypeConverter implements Converter<EsbFilterType> {
    @Override
    public EsbFilterType convert(Method method, String input) {
        if (StringUtils.isNotEmpty(input)) {
            return EsbFilterType.valueOf(input.toUpperCase());
        } else {
            return EsbFilterType.NONE;
        }
    }
}
