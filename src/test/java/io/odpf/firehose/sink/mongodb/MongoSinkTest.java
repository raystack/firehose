package io.odpf.firehose.sink.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteError;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import io.odpf.firehose.config.enums.SinkType;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.mongodb.request.MongoRequestHandler;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class MongoSinkTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private MongoClient client;

    @Mock
    private MongoRequestHandler mongoRequestHandler;

    @Mock
    private MongoCollection<Document> mongoCollection;

    private List<Message> messages;
    private final List<String> mongoRetryStatusCodeBlacklist = new ArrayList<>();

    @Before
    public void setUp() {
        initMocks(this);

        String jsonString = "{\"customer_id\":\"544131618\",\"categories\":[{\"category\":\"COFFEE_SHOP\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"PIZZA_PASTA\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"ROTI\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"FASTFOOD\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"merchants\":[{\"merchant_id\":\"542629489\",\"merchant_uuid\":\"62598e60-1e5b-497c-b971-5a2bb0efb745\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542777412\",\"merchant_uuid\":\"0a84a08b-8a53-47f4-9e62-7b7c2316dd08\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542675785\",\"merchant_uuid\":\"daf41597-27d4-4475-b7c7-4f11563adcdb\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1},{\"merchant_id\":\"542704646\",\"merchant_uuid\":\"9b522ca0-3ff0-4591-b60b-0e84b48d6d12\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542809106\",\"merchant_uuid\":\"b902f7ba-ab5e-4de1-9755-56648f556265\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1}],\"brands\":[{\"brand_id\":\"e9f7c4b2-4fa6-489a-ab20-a1bb4638ad29\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"336eb59c-621a-4704-811c-e1024f970e2e\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"0f30e2ca-f97f-43ec-895c-0d9d729e4cca\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"901af18e-f5b7-43c5-9e67-4906d6ccce51\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"da07057d-7fe1-47de-8713-4c1edcfc9afc\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0}],\"orders_4_weeks\":2,\"orders_24_weeks\":2,\"merchant_visits_4_weeks\":4,\"app_version_major\":\"3\",\"app_version_minor\":\"30\",\"app_version_patch\":\"2\",\"current_country\":\"ID\",\"os\":\"Android\",\"wallet_id\":\"16230097256391350739\",\"dag_run_time\":\"2019-06-27T07:27:00+00:00\"}";
        Message messageWithJSON = new Message(null, jsonString.getBytes(), "", 0, 1);

        String logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
        Message messageWithProto = new Message(null, Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        messages = new ArrayList<>();
        this.messages.add(messageWithJSON);
        this.messages.add(messageWithProto);

        mongoRetryStatusCodeBlacklist.add("11000");
        mongoRetryStatusCodeBlacklist.add("502");

        when(mongoRequestHandler.getRequest(messageWithJSON)).thenReturn(new ReplaceOneModel<>(
                new Document("customer_id", "35452"),
                new Document(),
                new ReplaceOptions().upsert(true)));
        when(mongoRequestHandler.getRequest(messageWithProto)).thenReturn(new ReplaceOneModel<>(
                new Document("customer_id", "35452"),
                new Document()));
    }

    @Test
    public void shouldGetRequestForEachMessageInEsbMessagesList() {
        MongoSink mongoSink = new MongoSink(instrumentation, SinkType.MONGODB.name(), mongoCollection, client, mongoRequestHandler,
                mongoRetryStatusCodeBlacklist);

        mongoSink.prepare(messages);
        verify(mongoRequestHandler, times(1)).getRequest(messages.get(0));
        verify(mongoRequestHandler, times(1)).getRequest(messages.get(1));
    }

    @Test
    public void shouldReturnEmptyArrayListWhenBulkResponseExecutedSuccessfully() {
        List<BulkWriteError> bulkWriteErrors = new ArrayList<>();
        MongoSinkMock mongoSinkMock = new MongoSinkMock(instrumentation, SinkType.MONGODB.name(), mongoCollection, client, mongoRequestHandler,
                mongoRetryStatusCodeBlacklist);
        mongoSinkMock.setBulkWriteErrors(bulkWriteErrors);

        List<Message> failedMessages = mongoSinkMock.pushMessage(this.messages);
        Assert.assertEquals(0, failedMessages.size());
    }


    @Test
    public void shouldReturnEsbMessagesListWhenBulkResponseHasFailuresAndEmptyBlacklist() {
        BulkWriteError bulkWriteError1 = new BulkWriteError(400, "DB not found", new BasicDBObject(), 0);
        BulkWriteError bulkWriteError2 = new BulkWriteError(400, "DB not found", new BasicDBObject(), 1);
        List<BulkWriteError> bulkWriteErrors = Arrays.asList(bulkWriteError1, bulkWriteError2);
        MongoSinkMock mongoSinkMock = new MongoSinkMock(instrumentation, SinkType.MONGODB.name(), mongoCollection, client, mongoRequestHandler,
                new ArrayList<>());
        mongoSinkMock.setBulkWriteErrors(bulkWriteErrors);

        List<Message> failedMessages = mongoSinkMock.pushMessage(this.messages);
        Assert.assertEquals(messages.get(0), failedMessages.get(0));
        Assert.assertEquals(messages.get(1), failedMessages.get(1));
    }

    @Test
    public void shouldReturnEsbMessagesListWhenBulkResponseHasFailuresWithStatusOtherThanBlacklist() {
        BulkWriteError bulkWriteError1 = new BulkWriteError(400, "DB not found", new BasicDBObject(), 0);
        BulkWriteError bulkWriteError2 = new BulkWriteError(400, "DB not found", new BasicDBObject(), 1);
        List<BulkWriteError> bulkWriteErrors = Arrays.asList(bulkWriteError1, bulkWriteError2);
        MongoSinkMock mongoSinkMock = new MongoSinkMock(instrumentation, SinkType.MONGODB.name(), mongoCollection, client, mongoRequestHandler,
                mongoRetryStatusCodeBlacklist);
        mongoSinkMock.setBulkWriteErrors(bulkWriteErrors);

        List<Message> failedMessages = mongoSinkMock.pushMessage(this.messages);
        Assert.assertEquals(messages.get(0), failedMessages.get(0));
        Assert.assertEquals(messages.get(1), failedMessages.get(1));
    }

    @Test
    public void shouldReturnEmptyMessageListIfAllTheResponsesBelongToBlacklistStatusCode() {
        BulkWriteError bulkWriteError1 = new BulkWriteError(11000, "Duplicate Key Error", new BasicDBObject(), 0);
        BulkWriteError bulkWriteError2 = new BulkWriteError(11000, "Duplicate Key Error", new BasicDBObject(), 0);
        List<BulkWriteError> bulkWriteErrors = Arrays.asList(bulkWriteError1, bulkWriteError2);
        MongoSinkMock mongoSinkMock = new MongoSinkMock(instrumentation, SinkType.MONGODB.name(), mongoCollection, client, mongoRequestHandler,
                mongoRetryStatusCodeBlacklist);
        mongoSinkMock.setBulkWriteErrors(bulkWriteErrors);

        List<Message> failedMessages = mongoSinkMock.pushMessage(this.messages);
        Assert.assertEquals(0, failedMessages.size());
    }

    @Test
    public void shouldReportTelemetryIfTheResponsesBelongToBlacklistStatusCode() {
        BulkWriteError bulkWriteError1 = new BulkWriteError(11000, "Duplicate Key Error", new BasicDBObject(), 0);
        BulkWriteError bulkWriteError2 = new BulkWriteError(11000, "Duplicate Key Error", new BasicDBObject(), 0);
        List<BulkWriteError> bulkWriteErrors = Arrays.asList(bulkWriteError1, bulkWriteError2);
        MongoSinkMock mongoSinkMock = new MongoSinkMock(instrumentation, SinkType.MONGODB.name(), mongoCollection, client, mongoRequestHandler,
                mongoRetryStatusCodeBlacklist);
        mongoSinkMock.setBulkWriteErrors(bulkWriteErrors);

        mongoSinkMock.pushMessage(this.messages);
        verify(instrumentation, times(2)).logInfo("Not retrying due to response status: {} is under blacklisted status code", "11000");
        verify(instrumentation, times(2)).logInfo("Message dropped because of status code: 11000");
        verify(instrumentation, times(2)).incrementCounterWithTags("firehose_sink_messages_drop_total", "cause=Duplicate Key Error");
    }

    @Test
    public void shouldReturnFailedMessagesIfSomeOfTheFailuresDontBelongToBlacklist() {
        BulkWriteError bulkWriteError1 = new BulkWriteError(11000, "Duplicate Key Error", new BasicDBObject(), 0);
        BulkWriteError bulkWriteError2 = new BulkWriteError(400, "DB not found", new BasicDBObject(), 0);
        BulkWriteError bulkWriteError3 = new BulkWriteError(502, "Collection not found", new BasicDBObject(), 0);

        List<BulkWriteError> bulkWriteErrors = Arrays.asList(bulkWriteError1, bulkWriteError3, bulkWriteError2);
        MongoSinkMock mongoSinkMock = new MongoSinkMock(instrumentation, SinkType.MONGODB.name(), mongoCollection, client, mongoRequestHandler,
                mongoRetryStatusCodeBlacklist);
        mongoSinkMock.setBulkWriteErrors(bulkWriteErrors);

        String logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
        Message messageWithProto = new Message(null, Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);
        messages.add(messageWithProto);
        List<Message> failedMessages = mongoSinkMock.pushMessage(this.messages);

        verify(instrumentation, times(2)).incrementCounterWithTags(any(String.class), any(String.class));
        Assert.assertEquals(1, failedMessages.size());
    }

    @Test
    public void shouldLogBulkRequestFailedWhenBulkResponsesHasFailures() {
        BulkWriteError bulkWriteError1 = new BulkWriteError(11000, "Duplicate Key Error", new BasicDBObject(), 0);
        BulkWriteError bulkWriteError2 = new BulkWriteError(11000, "Duplicate Key Error", new BasicDBObject(), 0);
        List<BulkWriteError> bulkWriteErrors = Arrays.asList(bulkWriteError1, bulkWriteError2);
        MongoSinkMock mongoSinkMock = new MongoSinkMock(instrumentation, SinkType.MONGODB.name(), mongoCollection, client, mongoRequestHandler,
                mongoRetryStatusCodeBlacklist);
        mongoSinkMock.setBulkWriteErrors(bulkWriteErrors);

        mongoSinkMock.pushMessage(this.messages);
        verify(instrumentation, times(1)).logWarn("Bulk request failed");
        verify(instrumentation, times(1)).logWarn("Bulk request failed count: {}", 2);
    }

    @Test
    public void shouldNotLogBulkRequestFailedWhenBulkResponsesHasNoFailures() {
        List<BulkWriteError> bulkWriteErrors = new ArrayList<>();

        MongoSinkMock mongoSinkMock = new MongoSinkMock(instrumentation, SinkType.MONGODB.name(), mongoCollection, client, mongoRequestHandler,
                mongoRetryStatusCodeBlacklist);
        mongoSinkMock.setBulkWriteErrors(bulkWriteErrors);

        mongoSinkMock.pushMessage(this.messages);
        verify(instrumentation, times(0)).logWarn("Bulk request failed");
        verify(instrumentation, times(0)).logWarn("Bulk request failed count: {}", 2);
    }

    public static class MongoSinkMock extends MongoSink {

        private List<BulkWriteError> bulkWriteErrors;

        public MongoSinkMock(Instrumentation instrumentation, String sinkType, MongoCollection<Document> mongoCollection, MongoClient mongoClient,
                             MongoRequestHandler mongoRequestHandler, List<String> mongoRetryStatusCodeBlacklist) {
            super(instrumentation, sinkType, mongoCollection, mongoClient, mongoRequestHandler, mongoRetryStatusCodeBlacklist);
        }

        public void setBulkWriteErrors(List<BulkWriteError> bulkWriteErrors) {
            this.bulkWriteErrors = bulkWriteErrors;
        }

        @Override
        protected  List<BulkWriteError> processRequest(List<WriteModel<Document>> bulkRequest) {
            return bulkWriteErrors;
        }
    }
}

