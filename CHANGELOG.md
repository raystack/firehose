# Changelog

All notable changes to this project will be documented in this file. See [standard-version](https://github.com/conventional-changelog/standard-version) for commit guidelines.

## [0.1.3](https://github.com/odpf/firehose/releases/tag/v0.1.3) (2021-10-01)

### Fixes

- Remove newRelic library and its references
- Remove formatting of Http Sink Header keys

## [0.1.2](https://github.com/odpf/firehose/releases/tag/v0.1.2) (2021-09-23)

### Features

- Log http response body when log_level = debug
- Upgrade to gradle version 7.2
- The x- prefix from the header is removed

### Fixes

- Input Stream of httpbody to be read only once
- Handle Http Response Code Zero for retry
- Use runtimeclasspath instead of runtime to include all dependencies into the jar

## [0.1.1](https://github.com/odpf/firehose/releases/tag/v0.1.1) (2021-08-16)

### Chore

- Enable pool connections and configurations metrics for JDBC sink 

## [0.1.0](https://github.com/odpf/firehose/releases/tag/v0.1.0) (2021-06-21)

### Features

- Add log sink
- Add HTTP sink
- Add JDBC sink
- Add InfluxDB sink
- Add Redis sink
- Add ElasticSearch sink
- Add GRPC sink
- Add Prometheus sink 
