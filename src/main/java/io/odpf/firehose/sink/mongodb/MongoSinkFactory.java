package io.odpf.firehose.sink.mongodb;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.odpf.firehose.config.MongoSinkConfig;
import io.odpf.firehose.config.enums.SinkType;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.serializer.MessageToJson;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sink.SinkFactory;
import io.odpf.firehose.sink.mongodb.request.MongoRequestHandler;
import io.odpf.firehose.sink.mongodb.request.MongoRequestHandlerFactory;
import io.odpf.firehose.sink.mongodb.response.MongoResponseHandler;
import io.odpf.firehose.sink.mongodb.util.MongoSinkFactoryUtil;
import org.aeonbits.owner.ConfigFactory;
import org.bson.Document;

import java.util.List;
import java.util.Map;

/**
 * Sink factory to configure and create MongoDB sink.
 */
public class MongoSinkFactory implements SinkFactory {

    /**
     * Creates MongoDB sink.
     *
     * @param configuration  the configuration
     * @param statsDReporter the stats d reporter
     * @param stencilClient  the stencil client
     * @return created sink
     */
    @Override
    public Sink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        MongoSinkConfig mongoSinkConfig = ConfigFactory.create(MongoSinkConfig.class, configuration);
        Instrumentation instrumentation = new Instrumentation(statsDReporter, io.odpf.firehose.sink.mongodb.MongoSinkFactory.class);
        String mongoConfig = String.format("\n\tMONGO connection urls: %s\n\tMONGO DB name: %s\n\tMONGO Primary Key: %s\n\tMONGO message type: %s"
                        + "\n\tMONGO Collection Name: %s\n\tMONGO request timeout in ms: %s\n\tMONGO retry status code blacklist: %s"
                        + "\n\tMONGO update only mode: %s"
                        + "\n\tMONGO should preserve proto field names: %s",
                mongoSinkConfig.getSinkMongoConnectionUrls(), mongoSinkConfig.getSinkMongoDBName(), mongoSinkConfig.getSinkMongoPrimaryKey(), mongoSinkConfig.getSinkMongoInputMessageType(),
                mongoSinkConfig.getSinkMongoCollectionName(), mongoSinkConfig.getSinkMongoRequestTimeoutMs(), mongoSinkConfig.getSinkMongoRetryStatusCodeBlacklist(),
                mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable(), true);
        instrumentation.logDebug(mongoConfig);

        MongoRequestHandler mongoRequestHandler = new MongoRequestHandlerFactory(mongoSinkConfig, new Instrumentation(statsDReporter, MongoRequestHandlerFactory.class),
                mongoSinkConfig.getSinkMongoPrimaryKey(), mongoSinkConfig.getSinkMongoInputMessageType(),
                new MessageToJson(new ProtoParser(stencilClient, mongoSinkConfig.getInputSchemaProtoClass()), true, false)

        ).getRequestHandler();

        MongoClient mongoClient = MongoSinkFactoryUtil.buildMongoClient(mongoSinkConfig, instrumentation);
        MongoDatabase database = mongoClient.getDatabase(mongoSinkConfig.getSinkMongoDBName());
        MongoCollection<Document> collection = database.getCollection(mongoSinkConfig.getSinkMongoCollectionName());

        List<String> mongoRetryStatusCodeBlacklist = MongoSinkFactoryUtil.getStatusCodesAsList(mongoSinkConfig.getSinkMongoRetryStatusCodeBlacklist());
        instrumentation.logInfo("MONGO connection established");
        return new MongoSink(new Instrumentation(statsDReporter, MongoSink.class), SinkType.MONGODB.name().toLowerCase(), mongoClient, mongoRequestHandler,
                new MongoResponseHandler(collection, instrumentation, mongoRetryStatusCodeBlacklist));
    }
}
