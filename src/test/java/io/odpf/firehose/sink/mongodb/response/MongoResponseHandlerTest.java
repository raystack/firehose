package io.odpf.firehose.sink.mongodb.response;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.AllArgsConstructor;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class MongoResponseHandlerTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private MongoCollection<Document> mongoCollection;

    private List<WriteModel<Document>> request;

    private final List<String> mongoRetryStatusCodeBlacklist = new ArrayList<>();

    @Before
    public void setUp() {
        initMocks(this);

        mongoRetryStatusCodeBlacklist.add("11000");
        mongoRetryStatusCodeBlacklist.add("502");
        request = new ArrayList<>();

        request.add(new ReplaceOneModel<>(
                new Document("customer_id", "35452"),
                new Document(),
                new ReplaceOptions().upsert(true)));

        request.add(new ReplaceOneModel<>(
                new Document("customer_id", "35452"),
                new Document()));
    }

    @Test
    public void shouldReturnEmptyArrayListWhenBulkResponseExecutedSuccessfully() {
        MongoResponseHandler mongoResponseHandler = new MongoResponseHandler(mongoCollection, instrumentation,
                mongoRetryStatusCodeBlacklist);
        when(mongoCollection.bulkWrite(request)).thenReturn(new BulkWriteResultMock(true, 1, 1));
        List<BulkWriteError> failedMessages = mongoResponseHandler.processRequest(request);
        Assert.assertEquals(0, failedMessages.size());
    }


    @Test
    public void shouldReturnEsbMessagesListWhenBulkResponseHasFailuresAndEmptyBlacklist() {
        BulkWriteError writeError1 = new BulkWriteError(400, "DB not found", new BsonDocument(), 0);
        BulkWriteError writeError2 = new BulkWriteError(400, "DB not found", new BsonDocument(), 1);
        List<BulkWriteError> writeErrors = Arrays.asList(writeError1, writeError2);
        MongoResponseHandler mongoResponseHandler = new MongoResponseHandler(mongoCollection, instrumentation,
                new ArrayList<>());

        when(mongoCollection.bulkWrite(request)).thenThrow(new MongoBulkWriteException(new BulkWriteResultMock(false, 0, 0), writeErrors, null, new ServerAddress()));
        List<BulkWriteError> failedMessages = mongoResponseHandler.processRequest(request);
        Assert.assertEquals(writeErrors.get(0), failedMessages.get(0));
        Assert.assertEquals(writeErrors.get(1), failedMessages.get(1));
    }

    @Test
    public void shouldReturnEsbMessagesListWhenBulkResponseHasFailuresWithStatusOtherThanBlacklist() {
        BulkWriteError writeError1 = new BulkWriteError(400, "DB not found", new BsonDocument(), 0);
        BulkWriteError writeError2 = new BulkWriteError(400, "DB not found", new BsonDocument(), 1);
        List<BulkWriteError> writeErrors = Arrays.asList(writeError1, writeError2);
        MongoResponseHandler mongoResponseHandler = new MongoResponseHandler(mongoCollection, instrumentation,
                mongoRetryStatusCodeBlacklist);

        when(mongoCollection.bulkWrite(request)).thenThrow(new MongoBulkWriteException(new BulkWriteResultMock(false, 0, 0), writeErrors, null, new ServerAddress()));
        List<BulkWriteError> failedMessages = mongoResponseHandler.processRequest(request);
        Assert.assertEquals(writeErrors.get(0), failedMessages.get(0));
        Assert.assertEquals(writeErrors.get(1), failedMessages.get(1));
    }

    @Test
    public void shouldReportTelemetryIfTheResponsesBelongToBlacklistStatusCode() {
        BulkWriteError writeError1 = new BulkWriteError(11000, "Duplicate Key Error", new BsonDocument(), 0);
        BulkWriteError writeError2 = new BulkWriteError(11000, "Duplicate Key Error", new BsonDocument(), 0);
        List<BulkWriteError> writeErrors = Arrays.asList(writeError1, writeError2);
        MongoResponseHandler mongoResponseHandler = new MongoResponseHandler(mongoCollection, instrumentation,
                mongoRetryStatusCodeBlacklist);
        when(mongoCollection.bulkWrite(request)).thenThrow(new MongoBulkWriteException(new BulkWriteResultMock(false, 0, 0), writeErrors, null, new ServerAddress()));
        mongoResponseHandler.processRequest(request);

        verify(instrumentation, times(2)).logWarn("Non-retriable error due to response status: {} is under blacklisted status code", 11000);
        verify(instrumentation, times(2)).logInfo("Message dropped because of status code: 11000");
        verify(instrumentation, times(2)).incrementCounterWithTags("firehose_sink_messages_drop_total", "cause=Duplicate Key Error");
    }


    @Test
    public void shouldReportTelemetryIfSomeOfTheFailuresDontBelongToBlacklist() {
        BulkWriteError writeError1 = new BulkWriteError(400, "Duplicate Key Error", new BsonDocument(), 0);
        BulkWriteError writeError2 = new BulkWriteError(11000, "Duplicate Key Error", new BsonDocument(), 0);
        List<BulkWriteError> writeErrors = Arrays.asList(writeError1, writeError2);
        MongoResponseHandler mongoResponseHandler = new MongoResponseHandler(mongoCollection, instrumentation,
                mongoRetryStatusCodeBlacklist);
        when(mongoCollection.bulkWrite(request)).thenThrow(new MongoBulkWriteException(new BulkWriteResultMock(false, 0, 0), writeErrors, null, new ServerAddress()));
        mongoResponseHandler.processRequest(request);

        verify(instrumentation, times(1)).logWarn("Non-retriable error due to response status: {} is under blacklisted status code", 11000);
        verify(instrumentation, times(1)).logInfo("Message dropped because of status code: 11000");
        verify(instrumentation, times(1)).incrementCounterWithTags("firehose_sink_messages_drop_total", "cause=Duplicate Key Error");

    }


    @Test
    public void shouldReturnFailedMessagesIfSomeOfTheFailuresDontBelongToBlacklist() {
        BulkWriteError writeError1 = new BulkWriteError(11000, "Duplicate Key Error", new BsonDocument(), 0);
        BulkWriteError writeError2 = new BulkWriteError(400, "DB not found", new BsonDocument(), 0);
        BulkWriteError writeError3 = new BulkWriteError(502, "Collection not found", new BsonDocument(), 0);

        List<BulkWriteError> writeErrors = Arrays.asList(writeError1, writeError2, writeError3);
        MongoResponseHandler mongoResponseHandler = new MongoResponseHandler(mongoCollection, instrumentation,
                mongoRetryStatusCodeBlacklist);

        request.add(new ReplaceOneModel<>(
                new Document("customer_id", "35452"),
                new Document(),
                new ReplaceOptions().upsert(true)));

        when(mongoCollection.bulkWrite(request)).thenThrow(new MongoBulkWriteException(new BulkWriteResultMock(false, 0, 0),
                writeErrors, null, new ServerAddress()));

        List<BulkWriteError> failedMessages = mongoResponseHandler.processRequest(request);

        verify(instrumentation, times(2)).incrementCounterWithTags(any(String.class), any(String.class));
        Assert.assertEquals(1, failedMessages.size());
        Assert.assertEquals(writeErrors.get(1), failedMessages.get(0));

    }

    @Test
    public void shouldLogBulkRequestFailedWhenBulkResponsesHasFailures() {
        BulkWriteError writeError1 = new BulkWriteError(11000, "Duplicate Key Error", new BsonDocument(), 0);
        BulkWriteError writeError2 = new BulkWriteError(11000, "Duplicate Key Error", new BsonDocument(), 0);
        List<BulkWriteError> writeErrors = Arrays.asList(writeError1, writeError2);
        MongoResponseHandler mongoResponseHandler = new MongoResponseHandler(mongoCollection, instrumentation,
                mongoRetryStatusCodeBlacklist);
        when(mongoCollection.bulkWrite(request)).thenThrow(new MongoBulkWriteException(new BulkWriteResultMock(false, 0, 0), writeErrors, null, new ServerAddress()));
        mongoResponseHandler.processRequest(request);
        verify(instrumentation, times(1)).logWarn("Bulk request failed count: {}", 2);
    }

    @Test
    public void shouldNotLogBulkRequestFailedWhenBulkResponsesHasNoFailures() {
        List<BulkWriteError> writeErrors = new ArrayList<>();

        MongoResponseHandler mongoResponseHandler = new MongoResponseHandler(mongoCollection, instrumentation,
                mongoRetryStatusCodeBlacklist);
        when(mongoCollection.bulkWrite(request)).thenThrow(new MongoBulkWriteException(new BulkWriteResultMock(false, 0, 0), writeErrors, null, new ServerAddress()));
        mongoResponseHandler.processRequest(request);

        verify(instrumentation, times(0)).logWarn("Bulk request failed count: {}", 2);
    }

    @AllArgsConstructor
    public static class BulkWriteResultMock extends BulkWriteResult {

        private final boolean wasAcknowledged;
        private final int insertedCount;
        private final int modifiedCount;

        @Override
        public boolean wasAcknowledged() {
            return wasAcknowledged;
        }

        @Override
        public int getInsertedCount() {
            return insertedCount;
        }

        @Override
        public int getMatchedCount() {
            return 0;
        }

        @Override
        public int getDeletedCount() {
            return 0;
        }

        @Override
        public boolean isModifiedCountAvailable() {
            return true;
        }

        @Override
        public int getModifiedCount() {
            return modifiedCount;
        }

        @Override
        public List<BulkWriteUpsert> getUpserts() {
            return null;
        }
    }
}

