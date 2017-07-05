package com.gojek.esb.factory;

import com.gojek.esb.config.ApplicationConfiguration;
import com.gojek.esb.config.AuditConfig;
import com.gojek.esb.config.EsbConsumerConfig;
import com.gojek.esb.consumer.EsbGenericConsumer;
import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.exception.EglcConfigurationException;
import com.gojek.esb.filter.EsbMessageFilter;
import com.gojek.esb.filter.Filter;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.SinkFactory;
import com.gojek.esb.sink.db.DBSinkFactory;
import com.gojek.esb.sink.http.HttpSinkFactory;
import com.gojek.esb.sink.log.LogSinkFactory;
import com.gojek.esb.util.Clock;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LogConsumerFactory {

    private Map<String, String> config;
    private final ApplicationConfiguration appConfig;
    private final StatsDClient statsDClient;
    private final Clock clockInstance;
    private static final Logger LOGGER = LoggerFactory.getLogger(LogConsumerFactory.class);

    public LogConsumerFactory(ApplicationConfiguration appConfig, Map<String, String> config) {
        this.appConfig = appConfig;
        this.config = config;
        LOGGER.info("--------- Config ---------");
        LOGGER.info(appConfig.getKafkaAddress());
        LOGGER.info(appConfig.getKafkaTopic());
        LOGGER.info(appConfig.getConsumerGroupId());
        LOGGER.info(appConfig.getSinkType().name());
        LOGGER.info("--------- ------ ---------");
        statsDClient = new NonBlockingStatsDClient(getPrefix(appConfig.getDataDogPrefix()), appConfig.getDataDogHost(),
                appConfig.getDataDogPort(), appConfig.getDataDogTags().split(","));
        clockInstance = new Clock();
    }

    public LogConsumer buildConsumer() {
        EsbConsumerConfig esbConsumerConfig = ConfigFactory.create(EsbConsumerConfig.class, config);
        AuditConfig auditConfig = ConfigFactory.create(AuditConfig.class, System.getenv());
        Filter filter = new EsbMessageFilter(esbConsumerConfig);
        EsbGenericConsumer consumer = new GenericKafkaFactory().createConsumer(esbConsumerConfig, auditConfig, config, statsDClient, filter);

        String syncFactoryClass = appConfig.sinkFactoryClass();
        if (syncFactoryClass == null || syncFactoryClass.trim() == "") {
            syncFactoryClass = factoryClassFromDepricatedSyncConfig();
        }

        Sink sink = instantiateFactory(syncFactoryClass);


        return new LogConsumer(consumer, sink, statsDClient, clockInstance);
    }

    private String factoryClassFromDepricatedSyncConfig() {

        switch (appConfig.getSinkType()) {
            case DB:
                return DBSinkFactory.class.getName();
            case HTTP:
                return HttpSinkFactory.class.getName();
            case LOG:
                return LogSinkFactory.class.getName();
            default:
                throw new EglcConfigurationException("Sink factory class or sync type not defined");
        }
    }

    private Sink instantiateFactory(String sinkFactoryClass) {
        SinkFactory sinkFactory = null;
        try {
            sinkFactory = (SinkFactory) Class.forName(sinkFactoryClass).getConstructors()[0].newInstance();
        } catch (ReflectiveOperationException e) {
           throw new EglcConfigurationException("Bad configuration for sink", e);
        }
        return sinkFactory.create(System.getenv(), statsDClient);
    }

    private static String getPrefix(String prefix) {
        return "log.consumer." + prefix;
    }
}
