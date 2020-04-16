package com.gojek.esb.sink.grpc;


import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.grpc.response.GrpcResponse;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.grpc.client.GrpcClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * GrpcSink allows messages consumed from kafka to be relayed to a http service.
 * The related configurations for HTTPSink can be found here: {@see com.gojek.esb.config.HTTPSinkConfig}
 */
public class GrpcSink extends AbstractSink {

    private final GrpcClient grpcClient;

    private List<EsbMessage> esbMessages;

    public GrpcSink(Instrumentation instrumentation, GrpcClient grpcClient) {
    super(instrumentation, "grpc");
    this.grpcClient = grpcClient;
    }


    @Override
    protected List<EsbMessage> execute() throws Exception {
        ArrayList<EsbMessage> failedEsbMessages = new ArrayList<>();

        for (EsbMessage message : this.esbMessages) {
            GrpcResponse response = grpcClient.execute(message.getLogMessage(), message.getHeaders());
            if (!response.getSuccess()) {
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

    }
}
