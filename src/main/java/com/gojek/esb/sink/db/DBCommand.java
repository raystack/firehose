package com.gojek.esb.sink.db;

import java.sql.SQLException;
import java.util.List;

/**
 * Interface to execute queries against a database.
 */
public interface DBCommand {

    /**
     * method to execute the queries.
     *
     * @param queries sql queries to be executed.
     * @throws SQLException in case the query does not succeed.
     */
    void execute(List<String> queries) throws SQLException;

    void shutdownPool() throws InterruptedException;
}
