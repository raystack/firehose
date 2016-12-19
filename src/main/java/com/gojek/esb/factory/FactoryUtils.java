package com.gojek.esb.factory;

import com.gojek.esb.client.GenericHTTPClient;
import com.gojek.esb.config.ApplicationConfiguration;
import com.gojek.esb.parser.Header;
import com.gojek.esb.util.Clock;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FactoryUtils {
    private final static Logger logger = LoggerFactory.getLogger(FactoryUtils.class.getName());

    public static final ApplicationConfiguration appConfig = ConfigFactory.create(ApplicationConfiguration.class, System.getenv());

    public static final StatsDClient statsDClient = new NonBlockingStatsDClient(getPrefix(appConfig.getDataDogPrefix()), appConfig.getDataDogHost(),
            appConfig.getDataDogPort(), appConfig.getDataDogTags().split(","));

    public static final Clock clockInstance = new Clock();

    public static final GenericHTTPClient httpClient = new GenericHTTPClient(appConfig.getServiceURL(),
            Header.parse(appConfig.getHTTPHeaders()), statsDClient, clockInstance);

    private static String getPrefix(String prefix) {
        return "log.consumer." + prefix;
    }
}
