package io.odpf.firehose.sink.mongodb.response;

import com.mongodb.BulkWriteError;
import com.mongodb.BulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.WriteModel;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.AllArgsConstructor;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * The type Mongo response handler.
 */
@AllArgsConstructor
public class MongoResponseHandler {

    private final MongoCollection<Document> mongoCollection;
    private final Instrumentation instrumentation;

    /**
     * Process request list.
     *
     * @param bulkRequest the bulk request
     * @return the list of Bulk Write errors, if any, else returns empty list
     */
    public List<BulkWriteError> processRequest(List<WriteModel<Document>> bulkRequest) {
        List<BulkWriteError> bulkWriteErrors = new ArrayList<>();
        try {
            handleBulkWriteResult(mongoCollection.bulkWrite(bulkRequest), instrumentation);
        } catch (BulkWriteException bulkWriteException) {

            bulkWriteErrors = bulkWriteException.getWriteErrors();
        }
        return bulkWriteErrors;
    }

    private void handleBulkWriteResult(BulkWriteResult bulkWriteResult, Instrumentation instrumentation) {

        if (bulkWriteResult.wasAcknowledged()) {
            instrumentation.logInfo("Bulk Write operation was successfully acknowledged");
        } else {
            instrumentation.logInfo("Bulk Write operation was not acknowledged");
        }
    }
}
