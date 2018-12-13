package com.gojek.esb.sink.influxdb;

import com.gojek.esb.builder.PointBuilder;
import com.gojek.esb.config.InfluxSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.parser.ProtoParser;
import com.gojek.esb.sink.Sink;
import com.google.protobuf.DynamicMessage;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.gojek.esb.metrics.Metrics.*;

public class InfluxSink implements Sink {
    public static final String FIELD_NAME_MAPPING_ERROR_MESSAGE = "field index mapping cannot be empty; at least one field value is required";

    private InfluxDB client;
    private StatsDReporter statsDReporter;
    private ProtoParser protoParser;
    private InfluxSinkConfig config;
    private PointBuilder pointBuilder;

    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxSink.class);

    public InfluxSink(InfluxDB client, ProtoParser protoParser, InfluxSinkConfig config, StatsDReporter statsDReporter) {
        this.client = client;
        this.config = config;
        this.protoParser = protoParser;
        this.pointBuilder = new PointBuilder(config);
        this.statsDReporter = statsDReporter;
    }

    @Override
    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessageList) throws IOException {
        BatchPoints batchPoints = BatchPoints.database(config.getDatabaseName()).build();
        LOGGER.info("Started writing to influx db for {} records", esbMessageList.size());
        for (EsbMessage esbMessage : esbMessageList) {
            DynamicMessage message = protoParser.parse(esbMessage.getLogMessage());
            Point point = pointBuilder.buildPoint(message);
            batchPoints.point(point);
        }
        try {
            Instant startExecution = statsDReporter.getClock().now();
            client.write(batchPoints);
            statsDReporter.captureDurationSince(INFLUX_DB_SINK_WRITE_TIME, startExecution);
            statsDReporter.captureCount(INFLUX_DB_SINK_MESSAGES_COUNT, esbMessageList.size(), SUCCESS_TAG);
            LOGGER.info("Written {} records to influx", batchPoints.getPoints().size());
        } catch (Exception e) {
            LOGGER.warn("Failed to write {} records to influx", batchPoints.getPoints().size(), e);
            statsDReporter.captureCount(INFLUX_DB_SINK_MESSAGES_COUNT, esbMessageList.size(), FAILURE_TAG);
            return esbMessageList;
        }
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {

    }
}
