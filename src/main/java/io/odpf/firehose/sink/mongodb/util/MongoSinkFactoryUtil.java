package io.odpf.firehose.sink.mongodb.util;

import com.mongodb.ServerAddress;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@UtilityClass
public class MongoSinkFactoryUtil {

    public static List<ServerAddress> getServerAddresses(String mongoConnectionUrls, Instrumentation instrumentation) {
        if (mongoConnectionUrls != null && !mongoConnectionUrls.isEmpty()) {
            List<String> mongoNodes = Arrays.asList(mongoConnectionUrls.trim().split(","));
            List<ServerAddress> serverAddresses = new ArrayList<>(mongoNodes.size());
            mongoNodes.forEach((String mongoNode) -> {
                List<String> node = Arrays.stream(mongoNode.trim().split(":"))
                        .filter(nodeString -> !nodeString.isEmpty()).collect(Collectors.toList());
                if (node.size() <= 1) {
                    throw new IllegalArgumentException("SINK_MONGO_CONNECTION_URLS should contain host and port both");
                }
                serverAddresses.add(new ServerAddress(node.get(0).trim(), Integer.parseInt(node.get(1).trim())));
            });
            return serverAddresses;
        } else {
            instrumentation.logError("No connection URL found");
            throw new IllegalArgumentException("SINK_MONGO_CONNECTION_URLS is empty or null");
        }
    }
}
