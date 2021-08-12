# JDBC Sink

A JDBC sink Firehose \(`SINK_TYPE`=`jdbc`\) requires the following variables to be set along with Generic ones

## `SINK_JDBC_URL`

Deifnes the PostgresDB URL, it's usually the hostname followed by port.

* Example value: `jdbc:postgresql://localhost:5432/postgres`
* Type: `required`

## `SINK_JDBC_TABLE_NAME`

Defines the name of the table in which the data should be dumped.

* Example value: `public.customers`
* Type: `required`

## `SINK_JDBC_USERNAME`

Defines the username to connect to DB.

* Example value: `root`
* Type: `required`

## `SINK_JDBC_PASSWORD`

Defines the password to connect to DB.

* Example value: `root`
* Type: `required`

## `INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING`

Defines the mapping of fields in DB and the corresponding proto index from where the value will be extracted. This is a JSON field.

* Example value: `{"6":"customer_id","1":"service_type","5":"event_timestamp"}` Proto field value with index 1 will be stored in a column named service\_type in DB and so on
* Type: `required`

## `SINK_JDBC_UNIQUE_KEYS`

Defines a comma-separated column names having a unique constraint on the table.

* Example value: `customer_id`
* Type: `optional`

## `SINK_JDBC_CONNECTION_POOL_TIMEOUT_MS`

Defines a database connection timeout in milliseconds.

* Example value: `1000`
* Type: `required`
* Default value: `1000`

## `SINK_JDBC_CONNECTION_POOL_IDLE_TIMEOUT_MS`

Defines a database connection pool idle connection timeout in milliseconds.

* Example value: `60000`
* Type: `required`
* Default value: `60000`

## `SINK_JDBC_CONNECTION_POOL_MIN_IDLE`

Defines the minimum number of idle connections in the pool to maintain.

* Example value: `0`
* Type: `required`
* Default value: `0`

## `SINK_JDBC_CONNECTION_POOL_MAX_SIZE`

Defines the maximum size for the database connection pool.

* Example value: `10`
* Type: `required`
* Default value: `10`

