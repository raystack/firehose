package com.gojek.esb.sink.grpc;


import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.grpc.client.GrpcClient;
import com.google.protobuf.DynamicMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * GrpcSink allows messages consumed from kafka to be relayed to a http service.
 * The related configurations for HTTPSink can be found here: {@see com.gojek.esb.config.HTTPSinkConfig}
 */
public class GrpcSink extends AbstractSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcSink.class);
    private final GrpcClient grpcClient;

    private List<EsbMessage> esbMessages;

    private StencilClient stencilClient;



    public GrpcSink(Instrumentation instrumentation, GrpcClient grpcClient, StencilClient stencilClient) {
        super(instrumentation, "grpc");
        this.grpcClient = grpcClient;
        this.stencilClient = stencilClient;
    }


    @Override
    protected List<EsbMessage> execute() throws Exception {
        ArrayList<EsbMessage> failedEsbMessages = new ArrayList<>();

        for (EsbMessage message : this.esbMessages) {
            DynamicMessage response = grpcClient.execute(message.getLogMessage(), message.getHeaders());
            Object m = response.getField(response.getDescriptorForType().findFieldByName("success"));
            boolean success = (m != null) ? Boolean.valueOf(String.valueOf(m)) : false;

            if (!success) {
                LOGGER.info("Grpc Service returned error");
                failedEsbMessages.add(message);
            }
        }

        return failedEsbMessages;
    }

    @Override
    protected void prepare(List<EsbMessage> esbMessages2) throws DeserializerException {
        this.esbMessages = esbMessages2;
    }

    @Override
    public void close() throws IOException {
        this.esbMessages = new ArrayList<>();
        stencilClient.close();
    }
}
