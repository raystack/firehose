package io.odpf.firehose.sink.influxdb;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import io.odpf.firehose.sink.influxdb.builder.PointBuilder;
import io.odpf.firehose.config.InfluxSinkConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.metrics.Instrumentation;
import com.google.protobuf.DynamicMessage;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InfluxSink extends AbstractSink {
    public static final String FIELD_NAME_MAPPING_ERROR_MESSAGE = "field index mapping cannot be empty; at least one field value is required";

    private InfluxSinkConfig config;
    private ProtoParser protoParser;
    private PointBuilder pointBuilder;
    private InfluxDB client;
    private BatchPoints batchPoints;
    private StencilClient stencilClient;

    public InfluxSink(Instrumentation instrumentation, String sinkType, InfluxSinkConfig config, ProtoParser protoParser, InfluxDB client, StencilClient stencilClient) {
        super(instrumentation, sinkType);
        this.config = config;
        this.protoParser = protoParser;
        this.pointBuilder = new PointBuilder(config);
        this.client = client;
        this.stencilClient = stencilClient;
    }

    @Override
    protected void prepare(List<Message> messages) throws IOException {
        batchPoints = BatchPoints.database(config.getSinkInfluxDbName()).retentionPolicy(config.getSinkInfluxRetentionPolicy()).build();
        for (Message esbMessage : messages) {
            DynamicMessage message = protoParser.parse(esbMessage.getLogMessage());
            Point point = pointBuilder.buildPoint(message);
            getInstrumentation().logDebug("Data point: {}", point.toString());
            batchPoints.point(point);
        }
    }

    @Override
    protected List<Message> execute() {
        getInstrumentation().logDebug("Batch points: {}", batchPoints.toString());
        client.write(batchPoints);
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        getInstrumentation().logInfo("InfluxDB connection closing");
        stencilClient.close();
    }
}
