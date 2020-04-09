package com.gojek.esb.sink.grpc;


import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.grpc.response.GrpcResponse;
import com.gojek.esb.sink.Sink;
import com.gojek.esb.sink.grpc.client.GrpcClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HTTPSink allows messages consumed from kafka to be relayed to a http service.
 * The related configurations for HTTPSink can be found here: {@see com.gojek.esb.config.HTTPSinkConfig}
 */
public class GrpcSink implements Sink {

    private final GrpcClient grpcClient;

    public GrpcSink(GrpcClient grpcClient) {
        this.grpcClient = grpcClient;
    }

    @Override
    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessages) throws IOException, DeserializerException {
        ArrayList<EsbMessage> failedEsbMessages = new ArrayList<>();
        System.out.println("pushing messages: " + esbMessages.size());

        for (EsbMessage message : esbMessages) {
            System.out.println(message.getLogKey());
            System.out.println(message.getLogMessage());
            GrpcResponse response = grpcClient.execute(message.getLogMessage(), message.getHeaders());
            if (!response.getSuccess()) {
                failedEsbMessages.add(message);
            }
        }
        return failedEsbMessages;
    }

    @Override
    public void close() throws IOException {
    }
}
