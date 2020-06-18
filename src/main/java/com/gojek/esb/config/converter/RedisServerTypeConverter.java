package com.gojek.esb.config.converter;

import com.gojek.esb.config.enums.RedisServerType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class RedisServerTypeConverter implements Converter<RedisServerType> {
    @Override
    public RedisServerType convert(Method method, String input) {
        return RedisServerType.valueOf(input.toUpperCase());
    }
}
