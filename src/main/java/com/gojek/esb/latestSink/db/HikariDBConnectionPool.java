package com.gojek.esb.latestSink.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.pool.HikariPool;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Wraps Hikari database connection pool as a DBConnectionPool.
 */
public class HikariDBConnectionPool implements DBConnectionPool {

    private static final Integer CONNECTION_TIMEOUT_THRESHOLD = 250;
    private static final Integer IDLE_TIMEOUT_THRESHOLD = 0;

    private HikariPool hikariPool;

    public HikariDBConnectionPool(String jdbcUrl, String username, String password, int maximumPoolSize,
                                  long connectionTimeout, long idleTimeout, int minimumIdle) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(maximumPoolSize);
        config.setMinimumIdle(minimumIdle);
        if (connectionTimeout >= CONNECTION_TIMEOUT_THRESHOLD) {
            config.setConnectionTimeout(connectionTimeout);
        }
        if (idleTimeout >= IDLE_TIMEOUT_THRESHOLD) {
            config.setIdleTimeout(idleTimeout);
        }
        this.hikariPool = new HikariPool(config);

    }

    @Override
    public Connection get() throws SQLException {
        return hikariPool.getConnection();
    }

    @Override
    public void release(Connection connection) {
        hikariPool.evictConnection(connection);
    }

    @Override
    public void shutdown() throws InterruptedException {
        hikariPool.shutdown();
    }
}
