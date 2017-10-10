# Changelog
All notable changes to this project since version 4.7.0 will be documented in this file.

This project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
