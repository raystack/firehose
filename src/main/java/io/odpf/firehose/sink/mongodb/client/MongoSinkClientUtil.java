package io.odpf.firehose.sink.mongodb.client;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@UtilityClass
public class MongoSinkClientUtil {

    /**
     * Gets status codes as list.
     *
     * @param mongoRetryStatusCodeBlacklist the mongo retry status code blacklist
     * @return the status codes as list
     * @since 0.1
     */
    static List<Integer> getStatusCodesAsList(String mongoRetryStatusCodeBlacklist) {
        try {
            return Arrays
                    .stream(mongoRetryStatusCodeBlacklist.split(","))
                    .map(String::trim)
                    .filter(s -> (!s.isEmpty()))
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Status code must be an integer");
        }
    }

    /**
     * Check database exists or not.
     *
     * @param databaseName    the database name
     * @param mongoClient     the mongo client
     * @param instrumentation the instrumentation
     * @return the boolean
     */
    static boolean checkDatabaseExists(String databaseName, MongoClient mongoClient, Instrumentation instrumentation) {
        if (databaseName == null) {
            throw new IllegalArgumentException("Database name cannot be null");
        }

        boolean doesDBExist = true;
        if (!mongoClient.listDatabaseNames()
                .into(new ArrayList<>())
                .contains(databaseName)) {
            instrumentation.logInfo("Database: " + databaseName + " does not exist. Attempting to create database");
            doesDBExist = false;
        }
        return doesDBExist;
    }

    /**
     * Check collection exists or not..
     *
     * @param collectionName  the collection name
     * @param database        the database
     * @param instrumentation the instrumentation
     * @return the boolean
     */
    static boolean checkCollectionExists(String collectionName, MongoDatabase database, Instrumentation instrumentation) {
        if (collectionName == null) {
            throw new IllegalArgumentException("Collection name cannot be null");
        }

        boolean doesCollectionExist = true;
        if (!database.listCollectionNames()
                .into(new ArrayList<>())
                .contains(collectionName)) {
            doesCollectionExist = false;
            instrumentation.logInfo("Collection: " + collectionName + " does not exist. Attempting to create collection");
        }
        return doesCollectionExist;
    }
}
