package io.odpf.firehose.sink.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for database connection pool.
 */
public interface JdbcConnectionPool {
    /**
     * Get connection from the pool.
     *
     * @return the connection
     * @throws SQLException the sql exception
     */
    Connection get() throws SQLException;

    /**
     * Release the connection held.
     *
     * @param connection the connection
     */
    void release(Connection connection);

    /**
     * Shutdown the connection pool.
     *
     * @throws InterruptedException the interrupted exception
     */
    void shutdown() throws InterruptedException;
}
