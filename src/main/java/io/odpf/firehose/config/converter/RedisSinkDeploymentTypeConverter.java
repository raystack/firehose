package io.odpf.firehose.config.converter;

import io.odpf.firehose.config.enums.RedisSinkDeploymentType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class RedisSinkDeploymentTypeConverter implements Converter<RedisSinkDeploymentType> {
    @Override
    public RedisSinkDeploymentType convert(Method method, String input) {
        return RedisSinkDeploymentType.valueOf(input.toUpperCase());
    }
}
