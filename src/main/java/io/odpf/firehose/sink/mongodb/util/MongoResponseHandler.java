package io.odpf.firehose.sink.mongodb.util;

import com.mongodb.BulkWriteError;
import com.mongodb.BulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.WriteModel;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.experimental.UtilityClass;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

@UtilityClass
public class MongoResponseHandler {

    public static List<BulkWriteError> processRequest(List<WriteModel<Document>> bulkRequest, MongoCollection<Document> mongoCollection, Instrumentation instrumentation) {
        List<BulkWriteError> bulkWriteErrors = new ArrayList<>();
        try {
            handleBulkWriteResult(mongoCollection.bulkWrite(bulkRequest),instrumentation);
        } catch (BulkWriteException bulkWriteException) {

            bulkWriteErrors = bulkWriteException.getWriteErrors();
        }

        return bulkWriteErrors;
    }

    private static void handleBulkWriteResult(BulkWriteResult bulkWriteResult, Instrumentation instrumentation) {

        instrumentation.logInfo("Successfully inserted {} documents", bulkWriteResult.getInsertedCount());
        instrumentation.logInfo("Successfully updated {} documents", bulkWriteResult.getModifiedCount());

    }
}
