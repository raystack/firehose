# Retries

## `RETRY_EXPONENTIAL_BACKOFF_INITIAL_MS`

Initial expiry time in milliseconds for exponential backoff policy.

* Example value: `10`
* Type: `optional`
* Default value: `10`

## `RETRY_EXPONENTIAL_BACKOFF_RATE`

Backoff rate for exponential backoff policy.

* Example value: `2`
* Type: `optional`
* Default value: `2`

## `RETRY_EXPONENTIAL_BACKOFF_MAX_MS`

Maximum expiry time in milliseconds for exponential backoff policy.

* Example value: `60000`
* Type: `optional`
* Default value: `60000`

## `RETRY_FAIL_AFTER_MAX_ATTEMPTS_ENABLE`

Fail the firehose if the retries exceed

* Example value: `true`
* Type: `optional`
* Default value: `false`

## `RETRY_MAX_ATTEMPTS`

Max attempts for retries

* Example value: `3`
* Type: `optional`
* Default value: `2147483647`

