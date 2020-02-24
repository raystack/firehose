# Changelog
All notable changes to this project since version 4.7.0 will be documented in this file.

This project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [4.22.0] - 2020-02-18
### Added
  * support for OAuth2 to HTTP Sink
## [4.21.0] - 2018-01-31
### Added
  * support for sending latency and message count to statsd.On by default.
## [4.20.3] - 2017-12-19
### Changed
  * Upgraded esb-log-entites from 3.11.9 to 3.11.12
## [4.20.1] - 2017-12-07
### Changed
  * Upgraded esb-log-entites from 3.9.2 to 3.11.9
## [4.20.0] - 2017-12-06
### Changed
  * support for null keys in kafka messages
## [4.19.1] - 2017-11-22
### Removed
  * clevertap sink dependency removed. Shold add sink libs as a runtime dependency.
## [4.19.0] - 2017-11-22
### Changed
  * Fixed bug in HTTPSink where backoff was not invoked for error responses
  * Exponential Back off in HttpSink is now configuraable
  * DBSink has breaking config change. Following config keys need to be renamed:
    BACKOFF_RATE to EXPONENTIAL_BACKOFF_RATE
    INITIAL_EXPIRY_TIME_IN_MS to EXPONENTIAL_BACKOFF_INITIAL_BACKOFF_IN_MS
    MAXIMUM_EXPIRY_TIME_IN_MS to EXPONENTIAL_BACKOFF_MAXIMUM_BACKOFF_IN_MS
  * Upgraded esb-log-entites from 3.9.0 to 3.10.9
## [4.18.0] - 2017-11-06
### Changed
  * Upgraded esb-log-entites from 3.8.4 to 3.9.0
## [4.17.0] - 2017-10-25
### Changed
  * Upgraded esb-log-entites from 3.7.3 to 3.8.4
## [4.16.0] - 2017-10-09
### Added
  * Removed esb-log-entites reference from esb-log-consumer and added it here
## [4.15.0] - 2017-10-09
### Added
  * Clevertap Sink handles timestamps and durations
## [4.14.0] - 2017-10-09
### Added
  * Clevertap Sink with exponential backoff and retries
## [4.13.0] - 2017-10-09
### Added
  * Clevertap Sink bare bones version
### Changed
  * COMMIT_ONLY_CURRENT_PARTITIONS is true by default. Should not have any functional impact.
## [4.12.0] - 2017-09-27
### Added
  * DB Sink converts inner protobuf message as json into a single column
### Changed
  * Influx sink configuration for proto mapping changed to conform to standard sink mapping configuraiton
## [4.11.0] - 2017-09-22
### Added
  * upgraded to 3.6 version of esb log entites
## [4.10.0] - 2017-09-19
### Added
  * COMMIT_ONLY_CURRENT_PARTITIONS configuration defaults to true now.
## [4.9.0] - 2017-09-11
### Added
  * Added Audit columns to DB sink.(Update in esb-log-consumer)
    - To enable auditing in DB sink add `DB_SINK_AUDIT_ENABLED=true` as environment variable.
    - Columns are - `kafak_topic_name`, `kafka_audit_partition`, `kafka_audit_offset`.
### Changed
  * `build.gradle` to use dynamic updation of minor version of esb log consumer.

## [4.8.0] - 2017-08-30
### Changed
  * Added feedback_skip field to feedback-log in esb-log-entities

## [4.7.0] - 2017-08-17
### Changed
  * Updated `build.gradle` to use latest version of esb-log-consumer
