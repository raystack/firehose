package io.odpf.firehose.sink.prometheus.builder;

import com.gojek.de.stencil.parser.ProtoParser;
import io.odpf.firehose.consumer.Message;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import cortexpb.Cortex;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Builder for Cortex WriteRequest.
 */
public class WriteRequestBuilder {

    private Cortex.WriteRequest.Builder writeRequestBuilder = Cortex.WriteRequest.newBuilder();
    private TimeSeriesBuilder timeSeriesBuilder;
    private ProtoParser protoParser;

    /**
     * Instantiates a new Write request builder.
     *
     * @param timeSeriesBuilder the builder fot Cortex TimeSeries
     * @param protoParser       the proto parser
     */
    public WriteRequestBuilder(TimeSeriesBuilder timeSeriesBuilder, ProtoParser protoParser) {
        this.timeSeriesBuilder = timeSeriesBuilder;
        this.protoParser = protoParser;
    }

    /**
     * build a cortex write request object.
     *
     * @param messages                          the list of consumer message
     * @return                                  Cortex.WriteRequest
     * @throws InvalidProtocolBufferException   the exception on invalid protobuf
     */
    public Cortex.WriteRequest buildWriteRequest(List<Message> messages) throws InvalidProtocolBufferException {
        writeRequestBuilder.clear();
        List<Cortex.TimeSeries> sortedTimeSeriesList = new ArrayList<>();
        for (Message esbMessage : messages) {
            DynamicMessage message = protoParser.parse(esbMessage.getLogMessage());
            int partition = esbMessage.getPartition();
            List<Cortex.TimeSeries> timeSeriesList = timeSeriesBuilder.buildTimeSeries(message, partition);
            sortedTimeSeriesList.addAll(timeSeriesList);
        }
        sortedTimeSeriesList.sort(Comparator.comparing(o -> o.getSamplesList().get(0).getTimestampMs()));
        writeRequestBuilder.addAllTimeseries(sortedTimeSeriesList);
        return writeRequestBuilder.build();
    }
}
