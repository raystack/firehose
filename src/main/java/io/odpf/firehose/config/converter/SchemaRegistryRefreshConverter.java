package io.odpf.firehose.config.converter;

import java.lang.reflect.Method;

import org.aeonbits.owner.Converter;

import io.odpf.stencil.cache.SchemaRefreshStrategy;

public class SchemaRegistryRefreshConverter implements Converter<SchemaRefreshStrategy> {

  @Override
  public SchemaRefreshStrategy convert(Method method, String input) {
    switch (input.toUpperCase()) {
      case "LONG_POLLING":
        return SchemaRefreshStrategy.longPollingStrategy();
      case "VERSION_BASED_REFRESH":
        return SchemaRefreshStrategy.versionBasedRefresh();
      default:
        return SchemaRefreshStrategy.longPollingStrategy();
    }
  }
}
