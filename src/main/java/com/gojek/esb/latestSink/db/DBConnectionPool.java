package com.gojek.esb.latestSink.db;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for database connection pool.
 */
public interface DBConnectionPool {
    Connection get() throws SQLException;

    void release(Connection connection);

    void shutdown() throws InterruptedException;
}
